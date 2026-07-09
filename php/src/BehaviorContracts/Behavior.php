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
        $wrappedExec = static function (array $op, mixed $bound) use ($exec, &$results): array {
            $outcome = $exec($op, $bound);
            if (array_key_exists('ok', $outcome)) {
                $results[$op['id']] = $outcome['ok'];
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
        BehaviorFailure::raise('UNKNOWN_NODE_KIND', "body node '{$id}' is not componentRef/map/cond");
    }

    private static function nodeParent(\stdClass $n): ?string
    {
        $props = get_object_vars($n);
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
        if (array_key_exists('map', $props) || array_key_exists('cond', $props)) {
            return null;
        }
        return self::optStr($props['bindField'] ?? null);
    }

    private static function nodePolicy(\stdClass $n): ?string
    {
        $props = get_object_vars($n);
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
        if (array_key_exists('map', $props)) {
            return self::optStr(get_object_vars($props['map'])['relationKind'] ?? null);
        }
        if (array_key_exists('cond', $props)) {
            return null;
        }
        return self::optStr($props['relationKind'] ?? null);
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
}
