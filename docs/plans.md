# The statement the planner should have seen

```sql
where (? is null or id = ?)
  and (? is null or name = ?)
```

You omit a filter in one of two ways. You concatenate the clause in application code, or you freeze both arms in one statement. Neither one gives the planner a simple equality.

## You concatenate the clause

You append a fragment for each argument that arrived. The value becomes SQL text, and each combination of filters is a different string in the host language.

**You send**

```text
sql = "select * from t where 1 = 1"
if id is not None:
    sql += " and id = " + str(id)
```

**The planner receives**

```sql
select * from t where 1 = 1 and id = 3
```

## You keep one string for every call

`(? is null or id = ?)` stays in the statement when `id` is null and when it is a value. The null arm matches every row, so this is not an index equality. One cached plan serves both calls. You bind `id` twice, and both positions have to carry the same value.

**You send**

```sql
where (? is null or id = ?)
```

**The planner receives**

```sql
where (? is null or id = ?)
```

## When any filter might be missing

`optional` keeps each block you supplied and drops the rest, including a preceding `and` or `or`. You write the long form once. `{"id": 3}` reaches the planner as one bind. `{}` drops the predicate. A block stays only when every parameter in it is present, and `{{id}}...` becomes `in (?, ?)` for the elements you passed.

**You write**

```sql
--(id)--
select * from t where ({{id}} is null or id = {{id}})
```

**The planner receives**

```sql
select * from t where (id = ?)
```

## When a flag decides the clause

`optional_when` removes the body when you omit the flag, and the flag is not bound. `false` is present, so the body stays and the planner receives `where (id = ?)`. You write `optional_when({{apply}}, id = {{id}})` instead of the string you append today.

**You send**

```text
if apply:
    sql += " and id = ?"

where (? = true and id = ?)
```

**The planner receives**

```sql
select * from t
```

## When the same predicate repeats for each row

`optional_groups_or({{pairs}}, u.id = {{self.id}} and u.active = {{active}})` keeps one body in the statement. Two rows become two copies joined by `or`. `[]` removes the group, and the planner receives `select * from users u`. A header flag inside the body drops that comparison from every row and leaves the rest.

**You send**

```text
for row in pairs:
    sql += " or (u.id = ? and u.active = ?)"
```

**The planner receives**

```sql
select * from users u where ((u.id = ? and u.active = ?) or (u.id = ? and u.active = ?))
```

## The syntax

- [optional](optional.md) drops a block you did not pass, and keeps the binds for the block you did.
- [optional groups](groups.md) repeats one body for each row you supply.
- [Nested](nested.md) puts a flag around those blocks.
