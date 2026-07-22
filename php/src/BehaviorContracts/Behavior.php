<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/Behavior.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * Behavior.php — component-graph IR + runBehavior 統合実行 IF（PHP port）。
 *
 * TS 参照実装 `ts/src/behavior.ts` / Python port `behavior.py` の意味論を **完全一致** で
 * 移植（scp-ir-architecture.md §5–§7）。
 *
 * component-graph IR（`components[]{name, inputPorts, body[], output, plan}`）を、既存 COMMON の
 * `Plan::runPlan`（stage 実行・Skip 伝播・Policy Kind）+ `ExprEval::evaluate`（Expression IR）の
 * 上に実行する。専用コンポーネントの実装は handler registry（catalog名 → 実装。境界注入）で
 * 名前解決して委譲する。
 *
 * body ノード種:
 *   - componentRef: `{id, component, ports, parent?, bindField?, relationKind?, policy?}`
 *   - map:          `{id, map:{over, as, component, ports, when?, into?, batched?, parent?,
 *                    relationKind?, policy?}}`（when/into/batched は behaviorVersion 2）
 *   - cond:         `{id, cond:{if, then, else, parent?}}`（純 Expression、handler を呼ばない）
 *
 * behaviorVersion 2（bc#22）:
 *   - `map.when`   — per-element guard。`{cond:[when,true,false]}` へ lower して評価
 *     （strict-bool・非 bool は TYPE_MISMATCH で fail-closed）。false の要素は skip。
 *   - `map.into`   — zip-attach。結果は「over の各 guard 通過要素へ into キーで handler 結果を
 *     書き戻した augment 済みリスト」（over と同じ長さ・順序。skip 要素は無変更で pass through）。
 *     guard 通過要素が stdClass でなければ MAP_INTO_ELEMENT_NOT_OBJECT。
 *   - `map.batched`— guard 通過全要素の ports を先に評価し handler を 1 回だけ
 *     `handler(['items' => [<stdClass ports>…]], $ctx)` で呼ぶ。結果は items と同じ長さ・順序の
 *     リスト契約（違反は MAP_BATCH_RESULT_MISMATCH）。通過 0 件なら handler は呼ばれず空リスト。
 *   - handler ctx  — 全 handler 呼び出しに `['nodeId'=>…, 'component'=>…]`（map 非 batched は
 *     さらに `'bound'`）を渡す。追加のみ・既存 handler 互換。
 *
 * 値モデル: IR ノードは stdClass、list は PHP array（json_decode(.., false) 由来）。
 * scope は assoc array（束縛名 → runtime 値）。
 *
 * handler 型（境界注入・§7.1）:
 *   callable(array<string,mixed> $ports, array<string,mixed> $ctx): array
 *     戻り値 = ['ok' => Value] | ['error' => string]。$ctx は node identity
 *     ['nodeId'=>…, 'component'=>…] ＋ map 要素の束縛値 ['bound'=>...]（非 batched map）。
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

final class Behavior
{
    /**
     * runBehavior — component-graph IR の統合実行 IF（scp-ir-architecture.md §7）。
     *
     * @param \stdClass $ir       component-graph 可搬 IR（`{components:[...]}`, json_decode false 由来）。
     * @param array<string,callable> $handlers 専用コンポーネント handler registry（catalog名 → 実装。境界注入）。
     * @param array<string,mixed> $input エントリ component の inputPorts 束縛（param 値）。
     * @param string|null $entry 実行する component 名（省略時は先頭 component）。
     * @return mixed `output` を評価した最終 Value（Φ 合流）。
     * @throws BehaviorFailure UNKNOWN_COMPONENT / MAP_OVER_NOT_ARRAY / UNKNOWN_NODE_KIND / UNKNOWN_ENTRY。
     * @throws PlanFailure OP_FAILED 等（plan 実行由来）。
     * @throws ExprFailure port 式・output 評価由来。
     */
    public static function runBehavior(\stdClass $ir, array $handlers, array $input = [], ?string $entry = null): mixed
    {
        $components = get_object_vars($ir)['components'] ?? [];
        if (!is_array($components)) {
            $components = [];
        }

        $comp = null;
        if ($entry !== null) {
            foreach ($components as $c) {
                if ($c instanceof \stdClass && (get_object_vars($c)['name'] ?? null) === $entry) {
                    $comp = $c;
                    break;
                }
            }
        } elseif (count($components) > 0) {
            $comp = $components[0];
        }
        if (!($comp instanceof \stdClass)) {
            BehaviorFailure::raise('UNKNOWN_ENTRY', "component '" . ($entry ?? '<first>') . "' not found in IR");
        }

        $compProps = get_object_vars($comp);

        // `{opt:T}` と宣言された input port（PortSchema `required:false`）は **省略可**で、値域は
        // `T | null`。「キーごと省略」と「null を渡す」は同じ「値が無い」の 2 通りの綴りなので、省略
        // されたキーは null に束縛する（可搬 IR が既に持つ宣言を読むだけ）。required な port と未宣言
        // の名前は束縛しない: 未束縛のまま参照されれば UNKNOWN_BINDING（fail-closed はそのまま）。
        $declaredPorts = $compProps['inputPorts'] ?? null;
        if ($declaredPorts instanceof \stdClass) {
            foreach (get_object_vars($declaredPorts) as $portName => $portSchema) {
                if (!($portSchema instanceof \stdClass)) {
                    continue;
                }
                if ((get_object_vars($portSchema)['required'] ?? null) !== false) {
                    continue;
                }
                if (!array_key_exists((string) $portName, $input)) {
                    $input[(string) $portName] = null;
                }
            }
        }

        /** @var array<int,\stdClass> $body */
        $body = $compProps['body'] ?? [];

        // id → index。
        $idToIndex = [];
        foreach ($body as $i => $node) {
            $idToIndex[(string) (get_object_vars($node)['id'] ?? '')] = $i;
        }

        // body ノード → OpSpec（runPlan の staging に載せる）。parent は id → index に解決。
        $ops = [];
        foreach ($body as $node) {
            $id = (string) (get_object_vars($node)['id'] ?? '');
            $pid = self::nodeParent($node);
            $parent = ($pid !== null) ? ($idToIndex[$pid] ?? null) : null;
            $op = ['id' => $id, 'parent' => $parent];
            $bf = self::nodeBindField($node);
            if ($bf !== null) {
                $op['bindField'] = $bf;
            }
            $rk = self::nodeRelationKind($node);
            if ($rk !== null) {
                $op['relationKind'] = $rk;
            }
            $pol = self::nodePolicy($node);
            if ($pol !== null) {
                $op['policy'] = $pol;
            }
            $ops[] = $op;
        }

        // 各 op 実行時に参照するスコープ: input params + それまでに ok になった node 結果（id → Value）。
        $results = [];
        $baseScope = static function () use ($input, &$results): array {
            return array_merge($input, $results);
        };

        $exec = static function (array $op, mixed $_bound) use (&$body, $idToIndex, $handlers, $baseScope): array {
            $idx = $idToIndex[$op['id']];
            $node = $body[$idx];
            $props = get_object_vars($node);
            $kind = self::nodeKind($node);

            if ($kind === 'cond') {
                // Conditional は純 Expression。cond 演算子（[if,then,else]）へ lower して評価。
                $c = get_object_vars($props['cond']);
                $condNode = new \stdClass();
                $condNode->cond = [$c['if'] ?? null, $c['then'] ?? null, $c['else'] ?? null];
                return ['ok' => ExprEval::evaluate($condNode, $baseScope())];
            }

            if ($kind === 'map') {
                $m = get_object_vars($props['map']);
                $over = ExprEval::evaluate($m['over'] ?? null, $baseScope());
                if (!is_array($over)) {
                    BehaviorFailure::raise('MAP_OVER_NOT_ARRAY', "map '{$op['id']}': 'over' did not evaluate to an array");
                }
                $component = $m['component'] ?? null;
                if (!isset($handlers[$component])) {
                    BehaviorFailure::raise('UNKNOWN_COMPONENT', "component '{$component}' has no handler (fail-closed)");
                }
                $handler = $handlers[$component];
                $as = (string) ($m['as'] ?? '');
                $mports = $m['ports'] ?? new \stdClass();
                $ctx = ['nodeId' => $op['id'], 'component' => $component];
                $hasWhen = array_key_exists('when', $m);
                $batched = ($m['batched'] ?? null) === true;
                $hasInto = array_key_exists('into', $m);

                // per-element guard（v2）: `{cond:[when,true,false]}` へ lower（strict-bool）。
                $keep = static function (array $scope) use ($hasWhen, $m): bool {
                    if (!$hasWhen) {
                        return true;
                    }
                    $condNode = new \stdClass();
                    $condNode->cond = [$m['when'], true, false];
                    return ExprEval::evaluate($condNode, $scope) === true;
                };

                // Element Error Policy Kind（scp-error.md）。既定 error; 未知 kind は fail-closed。
                // skip は要素ごとの Failure がある文脈でのみ合法 — batched map は handler を 1 回だけ
                // 呼び結果リストを 1 つ受け取るため fail-closed。
                $elementPolicy = $m['elementPolicy'] ?? 'error';
                if ($elementPolicy !== 'error' && $elementPolicy !== 'skip') {
                    BehaviorFailure::raise('UNKNOWN_ELEMENT_POLICY', "map '{$op['id']}': unknown element policy '{$elementPolicy}' (fail-closed)");
                }
                if ($elementPolicy === 'skip' && $batched) {
                    BehaviorFailure::raise('ELEMENT_POLICY_NOT_APPLICABLE', "map '{$op['id']}': elementPolicy 'skip' needs a per-element Failure, but a batched map takes ONE outcome for the whole batch (fail-closed)");
                }

                $keptIdx = []; // guard を通過した over 内 index（into の整列用）
                $collected = [];
                if ($batched) {
                    // batched（v2）: guard 通過全要素の ports を先に評価し、handler を 1 回だけ呼ぶ。
                    $items = [];
                    foreach ($over as $i => $el) {
                        $scope = $baseScope();
                        $scope[$as] = $el;
                        if (!$keep($scope)) {
                            continue;
                        }
                        $items[] = (object) self::evalPorts($mports, $scope);
                        $keptIdx[] = $i;
                    }
                    if (count($items) > 0) {
                        $outcome = $handler(['items' => $items], $ctx);
                        if (array_key_exists('error', $outcome)) {
                            return $outcome; // policy Kind は runPlan が解釈
                        }
                        $r = $outcome['ok'];
                        if (!is_array($r) || !array_is_list($r) || count($r) !== count($items)) {
                            BehaviorFailure::raise(
                                'MAP_BATCH_RESULT_MISMATCH',
                                "map '{$op['id']}': batched handler must return a list aligned to items (want " . count($items) . ')'
                            );
                        }
                        $collected = $r;
                    }
                    // guard 全落ち: handler は呼ばれず $collected = []。
                } else {
                    foreach ($over as $i => $el) {
                        $scope = $baseScope();
                        $scope[$as] = $el;
                        if (!$keep($scope)) {
                            continue;
                        }
                        $ports = self::evalPorts($mports, $scope);
                        $outcome = $handler($ports, $ctx + ['bound' => $el]);
                        if (array_key_exists('error', $outcome)) {
                            // Element Error Policy（scp-error.md）: skip は失敗要素を落として続行
                            // （順序保持・leaf は Error Value を保持）; error は map の Component Failure へ昇格。
                            if ($elementPolicy === 'skip') {
                                continue;
                            }
                            return $outcome; // policy Kind は runPlan が解釈
                        }
                        $collected[] = $outcome['ok'];
                        $keptIdx[] = $i;
                    }
                }

                if (!$hasInto) {
                    return ['ok' => $collected];
                }

                // into（v2）: over と同じ長さ・順序の augment 済みリスト（skip 要素は無変更）。
                $into = (string) $m['into'];
                $augmented = [];
                $k = 0;
                foreach ($over as $i => $el) {
                    if ($k < count($keptIdx) && $keptIdx[$k] === $i) {
                        if (!($el instanceof \stdClass)) {
                            BehaviorFailure::raise(
                                'MAP_INTO_ELEMENT_NOT_OBJECT',
                                "map '{$op['id']}': 'into' requires object elements (element {$i} is not an object)"
                            );
                        }
                        $aug = clone $el;
                        $aug->{$into} = $collected[$k];
                        $augmented[] = $aug;
                        $k++;
                    } else {
                        $augmented[] = $el;
                    }
                }
                return ['ok' => $augmented];
            }

            if ($kind === 'fanout') {
                // fanout（v3）: over（id-list）→ dedup 済み 1 回の batched handler → dedupe/drop/strip →
                // connection {items, cursor:null}。整列制約（MAP_BATCH_RESULT_MISMATCH）は適用されない。
                $f = get_object_vars($props['fanout']);
                $over = ExprEval::evaluate($f['over'] ?? null, $baseScope());
                if (!is_array($over)) {
                    BehaviorFailure::raise('FANOUT_OVER_NOT_ARRAY', "fanout '{$op['id']}': 'over' did not evaluate to an array");
                }
                $component = $f['component'] ?? null;
                if (!isset($handlers[$component])) {
                    BehaviorFailure::raise('UNKNOWN_COMPONENT', "component '{$component}' has no handler (fail-closed)");
                }
                $handler = $handlers[$component];
                $as = (string) ($f['as'] ?? '');
                $fports = $f['ports'] ?? new \stdClass();
                $itemsPorts = [];
                foreach ($over as $el) {
                    $scope = $baseScope();
                    $scope[$as] = $el;
                    $itemsPorts[] = (object) self::evalPorts($fports, $scope);
                }
                $conn = new \stdClass();
                $conn->items = [];
                $conn->cursor = null;
                if (count($itemsPorts) > 0) {
                    $outcome = $handler(['items' => $itemsPorts], ['nodeId' => $op['id'], 'component' => $component]);
                    if (array_key_exists('error', $outcome)) {
                        return $outcome; // policy Kind は runPlan が解釈
                    }
                    $r = $outcome['ok'];
                    if (!is_array($r) || !array_is_list($r) || count($r) !== count($itemsPorts)) {
                        BehaviorFailure::raise(
                            'FANOUT_BATCH_RESULT_MISMATCH',
                            "fanout '{$op['id']}': batched handler must return a list aligned to the deduped id list (want " . count($itemsPorts) . ')'
                        );
                    }
                    $conn->items = self::fanoutDedupDrop($r, (string) $f['dedupeKey'], (string) $f['drop'], self::optStr($f['implicitSource'] ?? null));
                }
                return ['ok' => $conn];
            }

            // componentRef
            $component = $props['component'] ?? null;
            if (!isset($handlers[$component])) {
                BehaviorFailure::raise('UNKNOWN_COMPONENT', "component '{$component}' has no handler (fail-closed)");
            }
            $handler = $handlers[$component];
            $ports = self::evalPorts($props['ports'] ?? new \stdClass(), $baseScope());
            return $handler($ports, ['nodeId' => $op['id'], 'component' => $component]);
        };

        // runPlan は stage 実行・Skip 伝播・Policy Kind を担う。成功結果を results へ写す薄いラッパ。
        $wrappedExec = static function (array $op, mixed $bound) use ($exec, &$results, &$body, $idToIndex): array {
            $outcome = $exec($op, $bound);
            if (array_key_exists('ok', $outcome)) {
                // outType conformance（scp-error.md）: 宣言は主張であり、ここで（runtime が宣言型と
                // 返り値の両方を持つ地点で）検査する。不一致は構造化 Error Value を載せて loud に落とし、
                // 値は宣言型へ正規化する（int→float widen / 欠落 opt→null）ので results が de-box と一致する。
                $node = $body[$idToIndex[$op['id']]];
                $nodeProps = get_object_vars($node);
                $value = $outcome['ok'];
                if (array_key_exists('outType', $nodeProps)) {
                    $value = self::assertConformsToOutType($op['id'], $value, self::resultTypeOf($nodeProps, $nodeProps['outType']), 'result');
                }
                $results[$op['id']] = $value;
            }
            return $outcome;
        };

        $plan = $compProps['plan'] ?? null;
        $planSpec = null;
        if ($plan instanceof \stdClass) {
            $pp = get_object_vars($plan);
            $planSpec = [
                'groups' => $pp['groups'] ?? [],
                'concurrency' => $pp['concurrency'] ?? 1,
            ];
        }

        $run = Plan::runPlan($planSpec, $ops, $wrappedExec);

        // Skip されたノードは未生成表現を results に載せる（output の ref が読めるように）。
        foreach ($run['states'] as $i => $s) {
            if ($s !== null && ($s['status'] ?? null) === 'skipped') {
                $results[$ops[$i]['id']] = Plan::unproducedValue(self::nodeRelationKind($body[$i]));
            }
        }

        // output（Φ 合流）を Expression IR として評価。
        return ExprEval::evaluate($compProps['output'] ?? null, $baseScope());
    }

    // ── ノード種判定 ─────────────────────────────────────────────────────────
    private static function nodeKind(\stdClass $n): string
    {
        $props = get_object_vars($n);
        if (array_key_exists('fanout', $props)) {
            return 'fanout';
        }
        if (array_key_exists('map', $props)) {
            return 'map';
        }
        if (array_key_exists('cond', $props)) {
            return 'cond';
        }
        if (array_key_exists('component', $props)) {
            return 'componentRef';
        }
        $id = $props['id'] ?? '?';
        BehaviorFailure::raise('UNKNOWN_NODE_KIND', "body node '{$id}' is not componentRef/map/cond/fanout");
    }

    private static function nodeParent(\stdClass $n): ?string
    {
        $props = get_object_vars($n);
        if (array_key_exists('fanout', $props)) {
            return self::optStr(get_object_vars($props['fanout'])['parent'] ?? null);
        }
        if (array_key_exists('map', $props)) {
            return self::optStr(get_object_vars($props['map'])['parent'] ?? null);
        }
        if (array_key_exists('cond', $props)) {
            return self::optStr(get_object_vars($props['cond'])['parent'] ?? null);
        }
        return self::optStr($props['parent'] ?? null);
    }

    private static function nodeBindField(\stdClass $n): ?string
    {
        $props = get_object_vars($n);
        if (array_key_exists('map', $props) || array_key_exists('cond', $props) || array_key_exists('fanout', $props)) {
            return null;
        }
        return self::optStr($props['bindField'] ?? null);
    }

    private static function nodePolicy(\stdClass $n): ?string
    {
        $props = get_object_vars($n);
        if (array_key_exists('fanout', $props)) {
            return self::optStr(get_object_vars($props['fanout'])['policy'] ?? null);
        }
        if (array_key_exists('map', $props)) {
            return self::optStr(get_object_vars($props['map'])['policy'] ?? null);
        }
        if (array_key_exists('cond', $props)) {
            return null;
        }
        return self::optStr($props['policy'] ?? null);
    }

    private static function nodeRelationKind(\stdClass $n): ?string
    {
        $props = get_object_vars($n);
        if (array_key_exists('fanout', $props)) {
            return self::optStr(get_object_vars($props['fanout'])['relationKind'] ?? null);
        }
        if (array_key_exists('map', $props)) {
            return self::optStr(get_object_vars($props['map'])['relationKind'] ?? null);
        }
        if (array_key_exists('cond', $props)) {
            return null;
        }
        return self::optStr($props['relationKind'] ?? null);
    }

    /**
     * fanoutDedupDrop — THE ONE dedup/drop definition (behaviorVersion 3) — PHP twin of behavior.ts
     * fanoutDedupDrop. First-seen dedupe by the body's `dedupeKey` field + dangling drop + implicitSource
     * strip on the aligned raw list; returns the connection `items` (the caller wraps). Byte-equal to the
     * TS/Python/native definitions. Bodies are \stdClass; a null / non-object / absent-key body is dangling.
     *
     * @param list<mixed> $alignedBodies
     * @return list<mixed>
     */
    public static function fanoutDedupDrop(array $alignedBodies, string $dedupeKey, string $drop, ?string $implicitSource): array
    {
        $items = [];
        $seen = [];
        foreach ($alignedBodies as $body) {
            $isObj = $body instanceof \stdClass;
            $rec = $isObj ? get_object_vars($body) : null;
            $keyVal = ($rec !== null && array_key_exists($dedupeKey, $rec)) ? $rec[$dedupeKey] : null;
            $hasKey = $keyVal !== null;
            if (!$hasKey) {
                if ($drop === 'dangling') {
                    continue;
                }
                $items[] = $body; // drop:none で dangling を保持
                continue;
            }
            $seenKey = is_string($keyVal) ? ('s:' . $keyVal) : ('j:' . json_encode($keyVal));
            if (array_key_exists($seenKey, $seen)) {
                continue;
            }
            $seen[$seenKey] = true;
            if ($implicitSource !== null && $rec !== null && array_key_exists($implicitSource, $rec)) {
                $copy = clone $body;
                unset($copy->{$implicitSource});
                $items[] = $copy;
            } else {
                $items[] = $body;
            }
        }
        return $items;
    }

    private static function optStr(mixed $v): ?string
    {
        return is_string($v) ? $v : null;
    }

    /**
     * ports オブジェクトの各値を Expression IR として評価する（key はソートせず宣言順のまま）。
     * FORBIDDEN_KEY 等は ExprEval が投げる（fail-closed）。
     *
     * @param mixed $ports stdClass（ポート配線）。
     * @param array<string,mixed> $scope
     * @return array<string,mixed>
     */
    private static function evalPorts(mixed $ports, array $scope): array
    {
        $out = [];
        if ($ports instanceof \stdClass) {
            foreach (get_object_vars($ports) as $k => $v) {
                $out[$k] = ExprEval::evaluate($v, $scope);
            }
        }
        return $out;
    }

    // ── outType conformance（scp-error.md「The outType conformance check」）──────────────

    /** 観測された値の wire 型名（ErrorDetail.actualWireType の語彙）。 */
    private static function wireTypeName(mixed $v): string
    {
        if ($v === null) {
            return 'null';
        }
        if (is_bool($v)) {
            return 'bool';
        }
        if (is_int($v)) {
            return 'int';
        }
        if (is_float($v)) {
            return 'float';
        }
        if (is_string($v)) {
            return 'string';
        }
        if (is_array($v)) {
            return 'arr';
        }
        return 'obj';
    }

    /** 問題の値（stringify 済み・型復元のために再パースされることは無い）。 */
    private static function rawValueOf(mixed $v): string
    {
        if (is_string($v)) {
            return $v;
        }
        if (is_bool($v)) {
            return $v ? 'true' : 'false';
        }
        if ($v === null) {
            return 'null';
        }
        if (is_int($v)) {
            return (string) $v;
        }
        if (is_float($v)) {
            return Canonical::pyFloatRepr($v);
        }
        return Canonical::canonicalJson($v);
    }

    /**
     * PortableType（型記法）の正準表記。宣言型は IR の stdClass/文字列で来る。
     *
     * @param mixed $t
     */
    private static function portableTypeNotation(mixed $t): string
    {
        if (is_string($t)) {
            return $t;
        }
        if ($t instanceof \stdClass) {
            $props = get_object_vars($t);
            foreach (['opt', 'arr', 'map'] as $key) {
                if (array_key_exists($key, $props)) {
                    return $key . '(' . self::portableTypeNotation($props[$key]) . ')';
                }
            }
            if (array_key_exists('obj', $props) && $props['obj'] instanceof \stdClass) {
                $parts = [];
                foreach (get_object_vars($props['obj']) as $k => $ft) {
                    $parts[] = $k . ':' . self::portableTypeNotation($ft);
                }
                return 'obj{' . implode(',', $parts) . '}';
            }
        }
        return '?';
    }

    /**
     * ノードの宣言 outType を結果値の型へ正規化する。map ノードの outType は要素型なので
     * 結果は `{arr: 要素型}`（compiler と同じ読み方）。map 以外は結果型そのもの。
     *
     * @param array<string, mixed> $nodeProps
     * @param mixed $outType
     */
    private static function resultTypeOf(array $nodeProps, mixed $outType): mixed
    {
        if (array_key_exists('map', $nodeProps)) {
            $wrap = new \stdClass();
            $wrap->arr = $outType;
            return $wrap;
        }
        return $outType;
    }

    /**
     * @param mixed $expected
     * @return never
     */
    private static function conformFail(string $code, string $message, string $kind, string $nodeId, string $field, mixed $expected, mixed $actual, bool $hasActual): never
    {
        $detail = [
            'kind' => $kind,
            'model' => $nodeId,
            'field' => $field,
            'expectedType' => self::portableTypeNotation($expected),
        ];
        if ($hasActual) {
            $detail['actualWireType'] = self::wireTypeName($actual);
            $detail['rawValue'] = self::rawValueOf($actual);
        }
        ExprFailure::raise($code, $message, $detail);
    }

    /**
     * handler 結果を宣言 outType に照らして検査し、**その型の値へ正規化して返す**（scp-error.md）。
     * 意味論は emitter の de-box と厳密一致させる（検査だけでなく値も同じ形へ正規化する）:
     *   - float は int を受けて **値を float へ widen**（de-box の ParseFloat と同一・int のまま返すと発散）。
     *   - opt は null 許容。宣言 obj フィールドのキー欠落は opt なら null へ materialize
     *     （de-box の absent→None と同一・MISSING_PROP にしない）。required 欠落は MISSING_PROP のまま。
     *   - obj は宣言フィールドを検査し、未宣言の余剰キーはそのまま通す（本 ruling の対象外）。
     *
     * @param mixed $t
     * @return mixed 正規化後の値
     */
    private static function assertConformsToOutType(string $nodeId, mixed $v, mixed $t, string $field): mixed
    {
        if (is_string($t)) {
            if ($t === 'float' && is_int($v) && !is_bool($v)) {
                return (float) $v; // int → float へ widen（de-box: ParseFloat と同一）
            }
            $ok = match ($t) {
                'string' => is_string($v),
                'int' => is_int($v) && !is_bool($v),
                'float' => is_float($v),
                'bool' => is_bool($v),
                'null' => $v === null,
                default => true,
            };
            if (!$ok) {
                self::conformFail('TYPE_MISMATCH', "node '{$nodeId}': {$field}: expected {$t}, got " . self::wireTypeName($v), 'typeMismatch', $nodeId, $field, $t, $v, true);
            }
            return $v;
        }
        if (!($t instanceof \stdClass)) {
            return $v;
        }
        $props = get_object_vars($t);
        if (array_key_exists('opt', $props)) {
            if ($v === null) {
                return null;
            }
            return self::assertConformsToOutType($nodeId, $v, $props['opt'], $field);
        }
        if (array_key_exists('arr', $props)) {
            if (!is_array($v) || !array_is_list($v)) {
                self::conformFail('TYPE_MISMATCH', "node '{$nodeId}': {$field}: expected arr, got " . self::wireTypeName($v), 'typeMismatch', $nodeId, $field, $t, $v, true);
            }
            $out = [];
            foreach ($v as $i => $el) {
                $out[] = self::assertConformsToOutType($nodeId, $el, $props['arr'], "{$field}[{$i}]");
            }
            return $out;
        }
        if (array_key_exists('map', $props)) {
            if (!($v instanceof \stdClass)) {
                self::conformFail('TYPE_MISMATCH', "node '{$nodeId}': {$field}: expected map, got " . self::wireTypeName($v), 'typeMismatch', $nodeId, $field, $t, $v, true);
            }
            $out = new \stdClass();
            foreach (get_object_vars($v) as $k => $mv) {
                $out->{$k} = self::assertConformsToOutType($nodeId, $mv, $props['map'], "{$field}.{$k}");
            }
            return $out;
        }
        if (array_key_exists('obj', $props) && $props['obj'] instanceof \stdClass) {
            if (!($v instanceof \stdClass)) {
                self::conformFail('TYPE_MISMATCH', "node '{$nodeId}': {$field}: expected obj, got " . self::wireTypeName($v), 'typeMismatch', $nodeId, $field, $t, $v, true);
            }
            // 未宣言の余剰キーを保持しつつ宣言フィールドを overlay する（shallow clone）。
            $out = clone $v;
            $vProps = get_object_vars($v);
            foreach (get_object_vars($props['obj']) as $k => $ft) {
                if (!array_key_exists($k, $vProps)) {
                    if ($ft instanceof \stdClass && array_key_exists('opt', get_object_vars($ft))) {
                        $out->{$k} = null; // absent opt → null（de-box: absent→None）
                        continue;
                    }
                    self::conformFail('MISSING_PROP', "node '{$nodeId}': {$field}: missing property .{$k}", 'missingField', $nodeId, "{$field}.{$k}", $ft, null, false);
                }
                $out->{$k} = self::assertConformsToOutType($nodeId, $vProps[$k], $ft, "{$field}.{$k}");
            }
            return $out;
        }
        return $v;
    }
}
