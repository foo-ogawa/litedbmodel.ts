[**litedbmodel v1.0.1**](../README.md)

***

[litedbmodel](../globals.md) / CreatedMiddlewareClass

# Interface: CreatedMiddlewareClass\<S\>

Defined in: Middleware.ts:427

Type for the middleware class created by createMiddleware.
Provides typed access to state via getCurrentContext().

## Type Parameters

| Type Parameter | Description |
| ------ | ------ |
| `S` *extends* `object` | The state object type |

## Constructors

### Constructor

```ts
new CreatedMiddlewareClass(): Middleware & S;
```

Defined in: Middleware.ts:437

Constructor

#### Returns

[`Middleware`](../classes/Middleware.md) & `S`

## Methods

### getCurrentContext()

```ts
getCurrentContext(): S;
```

Defined in: Middleware.ts:429

Get the current request's state (creates new instance if none exists)

#### Returns

`S`

***

### run()

```ts
run<R>(fn: () => R): R;
```

Defined in: Middleware.ts:431

Run a function with a fresh middleware context

#### Type Parameters

| Type Parameter |
| ------ |
| `R` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `fn` | () => `R` |

#### Returns

`R`

***

### hasContext()

```ts
hasContext(): boolean;
```

Defined in: Middleware.ts:433

Check if a context exists for the current request

#### Returns

`boolean`

***

### clearContext()

```ts
clearContext(): void;
```

Defined in: Middleware.ts:435

Clear the current context (useful for testing)

#### Returns

`void`
