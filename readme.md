# Hive X
The extension for hive

# Feature
- transform extension using map/reduce udf/jvm dynamic language

# Transform Extension
hive的udf/udaf/udtf不足的地方是,没有对应的setup/(map/reduce)/cleanup三个接口,并没有map/reduce模型灵活:

- udf无法批量处理数据,比如依赖redis进行过滤,只能单条过滤
- udaf虽然可以处理多条,但是已经是分组之后

## UDF Like Map/Reduce
map:

```java
public interface UDF {
    void setup(HxContext context) throws Exception;
    void map(HxLazyStruct row, HxContext context) throws Exception;
    void cleanup(HxContext context) throws Exception;
}
```

reduce:

```java
public interface UDF {
    void setup(HxContext context) throws Exception;
    void reduceBegin(Object[] keys, HxContext context) throws Exception;
    void reduce(Object[] keys, HxLazyStruct row, HxContext context) throws Exception;
    void reduceEnd(Object[] keys, HxContext context) throws Exception;
    void cleanup(HxContext context) throws Exception;
}
```
