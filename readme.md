# Hive X
The extension for hive

# Feature
- transform extension using map/reduce udf/jvm dynamic language

# Transform Extension
hive的udf/udaf/udtf不足的地方是,没有对应的setup/(map/reduce)/cleanup三个接口,并没有map/reduce模型灵活:

- udf无法批量处理数据,比如依赖redis进行过滤,只能单条过滤
- udaf虽然可以处理多条,但是已经是分组之后

先准备一张hive表,为后面的例子做准备:

```sql
create table test_1 (a string, b string) row format delimited fields terminated by '\t';
```

## UDF Like Map/Reduce
### map udf

```java
public interface HxUDFMapper.UDF {
    void setup(HxContext context) throws Exception;
    void map(HxLazyStruct row, HxContext context) throws Exception;
    void cleanup(HxContext context) throws Exception;
}
```
见Example: [UDFMapper](./script-extension/src/main/java/com/github/dryangkun/hive/udf/example/UDFMapper.java)

Usage:

```sql
select transform(a, b) 
using '#mapudf com.github.dryangkun.hive.example.UDFMapper' 
as (x array<string>, y int, z struct<z1:long,z2:double>) 
from test_1
```
* transform using格式: '#mapudf classname[ arg1 arg2...]'
* 如果是自己开发的UDF,则需要先执行hive的'add jar ...'

### reduce udf

```java
public interface HxUDFReducer.UDF {
    void setup(HxContext context) throws Exception;
    void reduceBegin(Object[] keys, HxContext context) throws Exception;
    void reduce(Object[] keys, HxLazyStruct row, HxContext context) throws Exception;
    void reduceEnd(Object[] keys, HxContext context) throws Exception;
    void cleanup(HxContext context) throws Exception;
}
```
* 与hadoop reduce接口不同,变成了5个接口,使用reduceBegin/reduce/reduceEnd三个接口来实现同一批keys的处理,
通过这种方式来避免使用List,从而省内存

见Example: [UDFReducer](./script-extension/src/main/java/com/github/dryangkun/hive/udf/example/UDFReducer.java)

Usage:

```sql
from (
    select a, b 
    from test_1 
    cluster by a 
) a 
select transform(a, b) 
using '#redudf 0 com.github.dryangkun.hive.example.UDFReducer' 
as (x, y)
```
* transform using格式: '#redudf key-index1[,key-index2...] classname[ arg1 arg2...]'
* key-index: transform(...)括号内作为reduce过程中分组的key的下标,从0开始,多个使用逗号分隔

## JVM Dynamic Language Mapper/Reducer
除了写java udf以外,还可以使用jvm dynamic language来实现mapper/reducer,
目前只实现了javascript, 后续会增加groovy.

### JavaScript Mapper/Reducer
兼容jdk1.7(基于[rhino](https://developer.mozilla.org/en-US/docs/Mozilla/Projects/Rhino))和jdk1.8(基于[nashorn](http://www.oracle.com/technetwork/articles/java/jf14-nashorn-2126515.html)),建议是使用jdk8的nashorn.

mapper.js:

```javascript
function setup(context) {
}

function map(row, context) {
}

function cleanup(context) {
}
```
见Example: [mapper.js](./script-extension/src/main/resources/hx/javascript/example/mapper.js)

Usage:
```sql
select 
transform(a, b) 
using '#mapjs [r]hx/javascript/example/mapper.js' 
as (x array<string>, y int, z struct<z1:long,z2:double>) 
from test_1
```
* transform using格式: '#mapjs file-or-resource-js[ arg1 arg2...]'
* 如果js文件在classpath中,则使用'[r]'表示从classpath加载js文件
* 如果是自己开发的js mapper文件,则需要使用hive的'add file ...'

reducer.js:

```javascript
function setup(context) {
}

function reduce_begin(keys, context) {
}

function reduce(keys, row, context) {
}

function reduce_end(keys, context) {
}

function cleanup(context) {
}
```
见Example: [reduce.js](./script-extension/src/main/resources/hx/javascript/example/reducer.js)

Usage:
```sql
from (
select a, b from test_1 cluster by a 
) a 
select transform(a, b) using '#redjs 0 [r]hx/javascript/example/reducer.js' 
as (x, y)
```
* transform using格式: '#redjs key-index1[,key-index2...] file-or-resource-js[ arg1 arg2...]'
* key-index: transform(...)括号内作为reduce过程中分组的key的下标,从0开始,多个使用逗号分隔

