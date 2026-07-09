/**
 * WS2 (#22) authoring parse — the litedbmodel authoring surface (SemanticBehavior
 * declaration + eager public-API path) lowers through ONE bc compile path to one internal
 * Component-graph IR, effect is derived from the graph, SKIP-optional conditions lower to
 * a `cond` expression (fragment existence, not a new opcode), and the lower path
 * auto-applies the portability guard fail-closed (spec §2.4 / §7 / §9 / #22 AC).
 */

import { describe, it, expect } from 'vitest';
import {
  PortabilityError,
  runBehavior,
  type Handlers,
} from 'behavior-contracts';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileEager,
  assertComponentGraphPortable,
  eq,
  ge,
  ne,
  when,
  type In,
  type Recorded,
  type ComponentGraphIR,
} from '../../src/scp';

const L = components();

// ── A read behavior: Select + relation .map (spec §2.4 PostSearch) ────────────────────
class ReadBehaviors extends SemanticBehavior {
  PostSearch($: In<{ authorId: number; status?: string; since: string }>) {
    const posts = L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'created_at'],
      where: [
        eq($.authorId, $.authorId),
        when(ne($.status, null), () => eq($.status, $.status)), // SKIP-optional (§7)
        ge($.since, $.since),
      ],
      order: 'created_at DESC',
    });
    const authors = posts.map(($p: Recorded) =>
      L.Select({ table: 'users', select: ['id', 'name'], where: [eq($p.author_id, $p.author_id)] }),
    );
    return { posts, authors };
  }
}

// ── A write behavior: Insert (spec §2.4 CreatePost) ───────────────────────────────────
class WriteBehaviors extends SemanticBehavior {
  CreatePost($: In<{ authorId: number; title: string }>) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.authorId,
      'values.title': $.title,
      returning: 'id, title',
    });
  }
}

describe('WS2 authoring — read behavior (Select + relation .map)', () => {
  const contract = publishBehaviors(ReadBehaviors);

  it('publishes exactly the public methods as root components (name = method name)', () => {
    expect(contract.components.map((c) => c.name)).toEqual(['PostSearch']);
    expect(Object.keys(contract.methods)).toEqual(['PostSearch']);
  });

  it('lowers the Select leaf with the WS1 catalog ports (table/select/where/order)', () => {
    const body = contract.methods.PostSearch.component.body;
    const select = body.find((n) => 'component' in n && n.component === 'Select');
    expect(select).toBeDefined();
    const ports = (select as { ports: Record<string, unknown> }).ports;
    expect(ports.table).toBe('posts');
    expect(ports.select).toEqual({ arr: ['id', 'author_id', 'title', 'created_at'] });
    expect(ports.order).toBe('created_at DESC');
  });

  it('lowers the relation as a bc map node over the parent Select result', () => {
    const body = contract.methods.PostSearch.component.body;
    const map = body.find((n) => 'map' in n) as { map: { component: string; over: unknown } } | undefined;
    expect(map).toBeDefined();
    expect(map!.map.component).toBe('Select');
    // `over` references the parent Select node result (a wire), not the input.
    expect(map!.map.over).toEqual({ ref: [body[0].id] });
  });
});

describe('WS2 authoring — SKIP optional condition (fragment existence, not an opcode)', () => {
  it('lowers `when(ne(status,null), () => eq(status,status))` to a pure cond node', () => {
    const contract = publishBehaviors(ReadBehaviors);
    const select = contract.methods.PostSearch.component.body.find(
      (n) => 'component' in n && n.component === 'Select',
    ) as { ports: { where: { arr: unknown[] } } };
    const where = select.ports.where.arr;
    // Second condition is the SKIP-optional one → `{cond:[ne(status,null), eq(status,status), null]}`.
    expect(where[1]).toEqual({
      cond: [
        { ne: [{ ref: ['status'] }, null] },
        { eq: [{ ref: ['status'] }, { ref: ['status'] }] },
        null,
      ],
    });
    // The `cond` operator is in bc's closed set — no litedbmodel-local opcode.
    expect(Object.keys(where[1] as object)).toEqual(['cond']);
  });
});

describe('WS2 authoring — effect derivation (graph-derived, never authored)', () => {
  it('a read behavior (Select only) derives Query', () => {
    const contract = publishBehaviors(ReadBehaviors);
    expect(contract.methods.PostSearch.effect).toBe('query');
  });

  it('a write behavior (Insert) derives Command', () => {
    const contract = publishBehaviors(WriteBehaviors);
    expect(contract.methods.CreatePost.effect).toBe('command');
  });

  it('the write behavior lowers the Insert value-family ports (values.<field>)', () => {
    const contract = publishBehaviors(WriteBehaviors);
    const insert = contract.methods.CreatePost.component.body.find(
      (n) => 'component' in n && n.component === 'Insert',
    ) as { ports: Record<string, unknown> };
    expect(insert.ports['values.author_id']).toEqual({ ref: ['authorId'] });
    expect(insert.ports['values.title']).toEqual({ ref: ['title'] });
    expect(insert.ports.returning).toBe('id, title');
  });
});

describe('WS2 single compile path — eager public API ≡ SemanticBehavior declaration', () => {
  // Declaration path: a one-method class equivalent to the eager call below.
  class EagerEquivDecl extends SemanticBehavior {
    FindPost($: In<{ authorId: number; since: string }>) {
      return L.Select({
        table: 'posts',
        select: ['id', 'author_id', 'title'],
        where: [eq($.authorId, $.authorId), ge($.since, $.since)],
        order: 'created_at DESC',
      });
    }
  }

  it('produces byte-identical internal Component-graph IR for an equivalent query', () => {
    const decl = publishBehaviors(EagerEquivDecl);
    // Eager path: the SAME authoring body, funneled through compileEager (spec §9).
    const eager = compileEager('FindPost', ($: Recorded, l) =>
      l.Select({
        table: 'posts',
        select: ['id', 'author_id', 'title'],
        where: [eq($.authorId, $.authorId), ge($.since, $.since)],
        order: 'created_at DESC',
      }),
    );

    // Byte-identical component IR (the single-compile-path invariant — spec §9).
    expect(JSON.stringify(eager.components)).toBe(JSON.stringify(decl.components));
    // Same derived effect (read → query) on both paths.
    expect(eager.methods.FindPost.effect).toBe('query');
    expect(decl.methods.FindPost.effect).toBe('query');
  });

  it('the eager path runs the SAME native control-syntax scan (fail-closed)', () => {
    // A native `?:` inside the eager body is rejected by bc's source scan, exactly as in a
    // declaration method — the eager path has no unscanned-thunk hole.
    expect(() =>
      compileEager('Bad', ($: Recorded, l) =>
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        l.Select({ table: 'posts', select: ($ as { flag?: unknown }).flag ? ['id'] : ['*'] }),
      ),
    ).toThrow(/native control syntax/i);
  });
});

describe('WS2 guard auto-wiring — non-portable opcode is rejected fail-closed (#22 AC)', () => {
  it('assertComponentGraphPortable rejects an opcode outside bc closed set', () => {
    // A lowered-looking IR that smuggles a non-portable single-key operator into a port.
    const ir: ComponentGraphIR = {
      irVersion: 1,
      exprVersion: 2,
      components: [
        {
          name: 'Bogus',
          inputPorts: { x: { type: 'unknown', required: true } },
          body: [{ id: 'n0', component: 'Select', ports: { table: 'posts', bogus: { sqlRaw: [{ ref: ['x'] }] } } }],
          output: { ref: ['n0'] },
        },
      ],
    };
    expect(() => assertComponentGraphPortable(ir)).toThrow(PortabilityError);
    expect(() => assertComponentGraphPortable(ir)).toThrow(/sqlRaw/);
  });

  it('the lower path runs on a portable behavior without throwing', () => {
    expect(() => publishBehaviors(ReadBehaviors)).not.toThrow();
    expect(() => publishBehaviors(WriteBehaviors)).not.toThrow();
  });
});

describe('WS2 emitted IR is runBehavior-executable (seam to WS3)', () => {
  it('the lowered read behavior executes through bc runBehavior with stub handlers', () => {
    const contract = publishBehaviors(ReadBehaviors);
    // Stub SQL handlers (WS3 supplies the real driver-backed ones): Select returns a fixed
    // row list so the map relation and output assembly are exercised end-to-end.
    const handlers: Handlers = {
      Select: (ports) => {
        if (ports.table === 'posts') return { ok: [{ id: 1, author_id: 7, title: 'Hello' }] };
        return { ok: [{ id: 7, name: 'Ada' }] };
      },
    };
    // `status` is supplied (null = present) so the where-port expressions bind; the
    // SKIP fragment-tree derivation from the `cond` node is a WS1/WS3 backend-compile
    // concern, not a runBehavior-time port evaluation.
    const out = runBehavior(contract.ir, handlers, { authorId: 7, status: null, since: '2026-01-01' }, 'PostSearch') as {
      posts: unknown[];
      authors: unknown[];
    };
    expect(out.posts).toEqual([{ id: 1, author_id: 7, title: 'Hello' }]);
    expect(out.authors).toEqual([[{ id: 7, name: 'Ada' }]]);
  });
});
