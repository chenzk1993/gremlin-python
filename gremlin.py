#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Author : zhangliang
# @Time :  2020/7/29 13:55
# @Filename : gremlin

from gremlin_python.driver import client
from gremlin_python.structure.graph import Graph

from gremlin_python import statics

from gremlin_python.process.graph_traversal import *
from gremlin_python.process.strategies import *

from gremlin_python.process.traversal import T

# Cardinality 「single, list, set」 节点属性对应的value是单值，还是列表，或者set。
from gremlin_python.process.traversal import Cardinality
from gremlin_python.process.traversal import Column
from gremlin_python.process.traversal import Direction
from gremlin_python.process.traversal import Operator
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Pop
from gremlin_python.process.traversal import Scope
from gremlin_python.process.traversal import Barrier
from gremlin_python.process.traversal import Bindings
from gremlin_python.process.traversal import WithOptions
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import Order
from gremlin_python.process.traversal import Pick
from gremlin_python.process.anonymous_traversal import traversal

g = traversal().withRemote(DriverRemoteConnection('ws://gds-bp18oddp33wq14lc150010pub.graphdb.rds.aliyuncs.com:3734/gremlin', 'g', username="gdb", password="w16TuZeaRQdaRE"))

g.V().hasLabel('gdb_sample_person').drop().iterate()
g.V().hasLabel('gdb_sample_software').drop().iterate()
g.E().hasLabel('gdb_sample_knows').drop().iterate()
g.E().hasLabel('gdb_sample_created').drop().iterate()

g.addV('gdb_sample_person').property(T.id, "gdb_sample_marko").property('age', 28).property('name', 'marko').property(Cardinality.set_, 'height', 171).property(Cardinality.set_, 'height', 172).iterate()
g.addV('gdb_sample_person').property(T.id, 'gdb_sample_vadas').property('age', 29).property('name', 'vadas').property(Cardinality.set_, 'height', 180).iterate()
g.addV('gdb_sample_person').property(T.id, 'gdb_sample_josh').property('age', 32).property('name', 'josh').property(Cardinality.set_, 'height', 164).iterate()
g.addV('gdb_sample_person').property(T.id, 'gdb_sample_peter').property('age', 35).property('name', 'peter').property(Cardinality.set_, 'height', 189).iterate()
g.addV('gdb_sample_software').property(T.id, 'gdb_sample_lop').property('lang', 'java').property('name', 'lop').iterate()
g.addV('gdb_sample_software').property(T.id, 'gdb_sample_ripple').property('lang', 'python').property('name', 'ripple').iterate()

g.addV('gdb_sample_person').property(T.id, "7737277327").iterate()
# 更新年龄
g.V("gdb_sample_marko").property('age', 29).iterate()


# 查询所有顶点的label,并只打印前3个,tail后三个
# print(g.V().label().limit(3).toList())

# 查询label为gdb_sample_person, range查询返回的元素的范围，左闭右开。
# print(g.V().hasLabel('gdb_sample_person').range(1,3).toList())

# 跳过n个元素，获取剩余的全部元素
# print(g.V().hasLabel('gdb_sample_person').skip(2).toList())

# 返回所有age属性的map对，不存在age的顶点则返回空字典
# print(g.V().valueMap('age').toList())


# 查询所有顶点的属性
# print(g.V().properties().toList())

# 查询所有顶点的"lang"属性,如果无"lang"属性的顶点将跳过
# print(g.V().properties('lang').toList())

# 查询所有label为gdb_sample_person的顶点
# print(g.V().hasLabel('gdb_sample_person').toList())

# 查询所有label为gdb_sample_person且年龄等于29，按照name进行倒排的顶点id列表, Order排序方式有incr,desc,shuffle
# print(g.V().hasLabel('gdb_sample_person').has('age',P.eq(29)).order().by('name',Order.desc).id().toList())


# 多个排序规则进行排序
# g.V().hasLabel('person').order().by(outE('created').count(), asc).
#                                  by('age', asc).values('name')

# 查询所有顶点的属性名称,并去重
# print(g.V().properties().key().toSet())

# 查询所有顶点的属性值
# print(g.V().properties().value().toSet())

# 查询所有顶点的属性
# print(g.V().valueMap().toList())

# 查询所有顶点的"lang"属性
# print(g.V().properties('lang').value().toSet())

# 查询所有顶点的id
print(g.V().id().toList())

# 找到所有name精确匹配 [josh,marko] 集合内(josh和marko)的顶点，并且以键值对方式显示这些顶点
# print(g.V().has('name',P.within('josh','marko')).valueMap().toList())

# 找到所有name不在集合 [josh,marko]内的顶点，并且以键值对方式显示这些顶点
# print(g.V().has('name',P.without('josh','marko')).valueMap().toList())

# 查询label为"gdb_sample_person"且"age"属性值为"32"的顶点
# print(g.V().has('gdb_sample_person', 'age', 32).toList())

# 查询年龄为29的顶点
# print(g.V().has('age', 29).toList())

# 返回所有顶点的存在属性值为29的顶点
# print(g.V().properties().hasValue(29).id().toList())

# 返回没有"age"属性,并且获取他们的name
# print(g.V().hasNot('age').values('name').toList())

# 查询id为"gdb_sample_marko"的顶点
# print(g.V().filter_(hasId('gdb_sample_marko')).toList())


from gremlin_python.process.graph_traversal import *
# 统计label为gdb_sample_person的顶点个数
# print(g.V().hasLabel('gdb_sample_person').count().toList())

# 对label为gdb_sample_person的顶点按年龄分组进行计数
#print(g.V().hasLabel('gdb_sample_person').values('age').groupCount().toList())
# 两者等价
#print(g.V().hasLabel('gdb_sample_person').group().by('age').by(count()).toList())

# 按label分组后，获得他们的name
# print(g.V().group().by(T.label).by('name').toList())

# 按label分组后，获得他们的顶点
# print(g.V().group().by(T.label).toList())

# 判断年龄为32的顶点
# print(g.V().values('age').is_(P.lte(30)).toList())

# 过滤年龄在30,40间的年龄，between[a,b),inside(), outside和inside相反
# print(g.V().values('age').is_(P.between(30, 40)).toList())

# # 过滤年龄的最大值, 类似的还有min,mean,sum
# print(g.V().values('age').max().toList())

g.addE('gdb_sample_knows').property(T.id, "marko_knows_vadas").from_(g.V('gdb_sample_marko')).to(g.V('gdb_sample_vadas')).property('weight', 0.5).iterate()
g.addE('gdb_sample_knows').property(T.id, "marko_knows_josh").from_(g.V('gdb_sample_marko')).to(g.V('gdb_sample_josh')).property('weight', 1.0).iterate()
g.addE('gdb_sample_knows').property(T.id, "josh_knows_peter").from_(g.V('gdb_sample_josh')).to(g.V('gdb_sample_peter')).property('weight', 0.7).iterate()
g.addE('gdb_sample_knows').property(T.id, "peter_knows_marko").from_(g.V('gdb_sample_peter')).to(g.V('gdb_sample_marko')).property('weight', 0.4).iterate()

g.addE('gdb_sample_created').property(T.id, "marko_created_lop").from_(g.V('gdb_sample_marko')).to(g.V('gdb_sample_lop')).property('weight', 0.4).iterate()
g.addE('gdb_sample_created').property(T.id, "josh_created_lop").from_(g.V('gdb_sample_josh')).to(g.V('gdb_sample_lop')).property('weight', 0.4).iterate()
g.addE('gdb_sample_created').property(T.id, "josh_created_ripple").from_(g.V('gdb_sample_josh')).to(g.V('gdb_sample_ripple')).property('weight', 1.0).iterate()
g.addE('gdb_sample_created').property(T.id, "peter_created_lop").from_(g.V('gdb_sample_peter')).to(g.V('gdb_sample_lop')).property('weight', 0.2).iterate()



#查询所有边的id
# print(g.E().id().toList())

# 查询所有边的label
# print(g.E().label().toList())

# 访问顶点的out方向邻接点
# print(g.V().out().toList())

# 访问指定顶点gdb_sample_marko的out方向邻接点
# print(g.V('gdb_sample_marko').out().toList())


# 访问指定顶点gdb_sample_josh的IN方向邻接点
# print(g.V('gdb_sample_josh').in_().toList())

# 访问指定顶点gdb_sample_josh的双向邻接点
# print(g.V('gdb_sample_josh').both().toList())

# dedup() 去除结果集中重复的元素
# print(g.V('gdb_sample_josh').both().hasLabel('gdb_sample_person').dedup().toList())

# 通过dedup去除顶点年龄相同的值
# print(g.V().hasLabel('gdb_sample_person').values('age').dedup().toList())

#访问某个顶点的out方向邻接边的id列表
# print(g.V('gdb_sample_josh').outE().id().toList())

#访问某个顶点的out方向,指定边的名称的列表
# print(g.V('gdb_sample_josh').outE("gdb_sample_created").toList())

# 访问某个顶点的IN方向邻接边
# print(g.V('gdb_sample_josh').inE().toList())

# 访问某个顶点的双向邻接边
# print(g.V('gdb_sample_josh').bothE().toList())

# 访问gdb_sample_josh顶点的IN邻接边，然后获取边的出顶点
# print(g.V('gdb_sample_josh').inE().outV().id().toList())

# 访问gdb_sample_josh顶点的outE邻接边，然后获取边的入顶点
# print(g.V('gdb_sample_josh').outE().inV().toList())

# 访问gdb_sample_josh顶点的out邻接边 然后获取边的双向顶点,并过滤出含有gdb_sample_josh的顶点
# bothV()会把源顶点也一起返回，因此只要源顶点有多少条出边，结果集中就会出现多少次源顶点
# print(g.V('gdb_sample_josh').outE().bothV().filter_(hasId("gdb_sample_josh")).toList())

# #访问gdb_sample_josh顶点的out方向邻接顶点列表, outE().otherV()等价于out(),inE().otherV()等价于in()
# print(g.V('gdb_sample_josh').outE().otherV().toList())

# inV
# print(g.V('gdb_sample_marko').outE('gdb_sample_knows').inV().hasLabel('gdb_sample_person').toList())
# print(g.V('gdb_sample_marko').outE('gdb_sample_knows').inV().hasLabel('gdb_sample_person').outE('gdb_sample_created').inV().hasLabel('gdb_sample_software').toList())

# 路径分为两种:有环路径和无环路径。
# 有环路径是指路径中至少有一个对象出现的次数大于等于两次。
# 无环路径是指路径中所有的对象只出现一次。

# path() 返回当前遍历过的所有路径。
# print(g.V().hasLabel('gdb_sample_person').has('name','josh').both().both().path().toList())

# simplePath()，过滤掉路径中含有环路的对象，只保留路径中不含有环路的对象
#print(g.V().hasLabel('gdb_sample_person').has('name','josh').both().simplePath().toList())

# cyclicPath()，过滤掉路径中不含有环路的对象，只保留路径中含有环路的对象
#print(g.V().hasLabel('gdb_sample_person').has('name','josh').both().both().cyclicPath().toList())

# repeat() + times()：按照指定的次数重复执行语句
#访问gdb_sample_josh顶点的out的2次邻接点
# print(g.V('gdb_sample_josh').repeat(out()).times(2).toList())

# 访问gdb_sample_josh顶点的IN的2次邻接点
# print(g.V('gdb_sample_josh').repeat(in_()).times(2).toList())

# repeat() + until()：根据条件来重复执行语句

# 查询顶点名称'marko'到顶点'gdb_sample_josh'之间的路径,循环的终止条件是遇到name是'marko'的顶点
# print(g.V('gdb_sample_josh').repeat(in_()).until(has('name', "marko")).path().toList())

# 查询顶点'gdb_sample_josh'到顶点'gdb_sample_marko'之间的路径,循环的终止条件是遇到id是'gdb_sample_marko'的顶点
# print(g.V('gdb_sample_josh').repeat(out()).until(hasId("gdb_sample_marko")).path().toList())

# repeat() + emit()：收集执行过程中的数据
# emit()放在repeat()之前或之后的顺序是会影响结果的，放前面表示先收集再执行，放后面表示先执行后收集。
# 查询顶点'gdb_sample_josh'的所有out可达点的路径, 且须满足是'gdb_sample_person'类型的点， 有问题,emit没起作用
# print(g.V('gdb_sample_josh').repeat(out()).emit(hasLabel('gdb_sample_person')).toList())

# print(g.V("gdb_sample_josh").repeat(out("gdb_sample_knows")).until(repeat(out("gdb_sample_created")).emit(has("name", "lop"))).toList())

#查询顶点'gdb_sample_josh'的out度为3的可达点路径
# print(g.V('gdb_sample_josh').repeat(out()).until(loops().is_(3)).path().toList())

# 查询顶点'okram'到顶点'marko''之间的路径且之间只相差2的距离
# print(g.V('gdb_sample_josh').repeat(out()).until(has('name', 'marko').and_().loops().is_(2)).path().toList())

# 查找从一个节点出发，到该节点的出度为3结束的所有路径
# print(g.V('gdb_sample_josh').repeat(out()).until(outE().count().is_(3)).path().toList())

# 查找从一个节点出发，到叶子节点结束的所有路径，有问题 .with_('scriptEvaluationTimeout', 50000000)
# print(g.V('gdb_sample_marko').repeat(out()).until(outE().count().is_(0)).path().toList())

# 统计gdb_sample_lop的出度
# print(g.V('gdb_sample_lop').outE().count().toList())

# 查找从一个节点出发，到该节点的入度为1结束的所有路径
# print(g.V('gdb_sample_josh').repeat(out()).until(inE().count().is_(1)).path().toList())


# 查找gdb_sample_josh节点出发，到gdb_sample_lop节点的所有路径,且循环次数小于等于4,若存在多条，第一条为最短路径
# print(g.V('gdb_sample_josh').repeat(out().simplePath())
#       .until(hasId('gdb_sample_lop').and_().loops()
#              .is_(P.lte(4))).path().toList())

# as_ 创建别名 选择从gdb_sample_josh出度为2的结果的路径，并重命名为a->c
# print(g.V('gdb_sample_josh').as_('a').out().as_('b').out().as_('c').select('a', 'c').toList())

# 选择从gdb_sample_josh出度为2的结果的路径，且出度为2时到达的顶点应与gdb_sample_josh的label相同
# print(g.V('gdb_sample_josh').as_('a').out().as_('b').out().as_('c').where('a', P.eq('c')).by(T.label).select('a', 'b', 'c').by(T.id).toList())

#输出从gdb_sample_josh节点出发，出度为2时的节点，并输出该节点的入节点
# print(g.V('gdb_sample_josh').as_('a').out().as_('b').out().as_('c').in_().path().toList())

# 输出从gdb_sample_josh节点出发，出度为2时的节点，并输出该节点的入节点，并按照对"a", "b", "c"进行去重，得到路径。
# print(g.V('gdb_sample_josh').as_('a').out().as_('b').out().as_('c').in_().dedup('a', 'b', 'c').path().toList())

# 输出从gdb_sample_josh节点出发，出度为1时的节点，并输出该节点的入节点重命名为'b'，且不等于'a'的顶点。
# print(g.V('gdb_sample_josh').as_('a').out().in_().as_('b').where('a',P.neq('b')).toList())

# 查询"被别人认识"且认识自己的人的年龄大于自己的年龄的人
# print(g.V().as_('a').out('gdb_sample_knows').as_('b').where('a', P.lt('b')).by('age').toList())


# math()支持的运算符包括：+，-，*，/，%，^
# math()支持的内嵌函数包括：
# abs: absolute value，绝对值
# acos: arc cosine，反余弦
# asin: arc sine，反正弦
# atan: arc tangent，反正切
# cbrt: cubic root，立方根
# ceil: nearest upper integer，向上最接近的整数
# cos: cosine，余弦
# cosh: hyperbolic cosine，双曲余弦
# exp: euler’s number raised to the power (e^x)，以e为底的指数
# floor: nearest lower integer，向下最近接的整数
# log: logarithmus naturalis (base e)，以e为底的对数
# log10: logarithm (base 10)，以10为底的对数
# log2: logarithm (base 2)，以2为底的对数
# sin: sine，正弦
# sinh: hyperbolic sine，双曲正弦
# sqrt: square root，平方根
# tan: tangent，正切
# tanh: hyperbolic tangent，双曲正切
# signum: signum function，签名功能

# 比如按照年龄段进行统计
# print(g.V().hasLabel('gdb_sample_person').groupCount().by(values('age').math('floor(_/5)*5')).order().by(Order.desc).toList())

# choose()的基本使用方法有两类：
#查找所有顶点，类型为"gdb_sample_person"的顶点输出其"name"属性,否则输出lang属性
# print(g.V().choose(hasLabel('gdb_sample_person'),values('name'),values('lang')).toList())

#查找所有顶点，类型为"gdb_sample_person"的顶点,对于marko，输出他的age，对于vadas，输出他的label, 其他顶点输出他们的年龄
# print(g.V().hasLabel('gdb_sample_person').choose(values('name')).option('marko', values('age')).option('vadas', label()).option(Pick.none, values('name')).toList())

# 查找所有顶点，对于有age属性的顶点，输出age, 否则输出lang属性
# print(g.V().choose(has('age'),values('age'),values('lang')).toList())

# coalesce: 可以接受任意数量的遍历器(traversal)，按顺序执行，并返回第一个能产生输出的遍历器的结果；
# 对gdb_sample_josh寻找输出边为gdb_sample_knows的顶点，并输出对应的路径，若没有则输出gdb_sample_created的对应路径
# print(g.V("gdb_sample_josh").coalesce(outE('gdb_sample_knows'), outE('gdb_sample_created')).inV().path().toList())

# 输出gdb_sample_josh的gdb_sample_created 和gdb_sample_knows出的所有路径
# print(g.V('gdb_sample_josh').union(out('gdb_sample_created'), out('gdb_sample_knows')).path().toList())

# 将gdb_sample_josh输出的边gdb_sample_created，聚合成x,并通过cap取出结果

# cap与select区别，cap只输出 out的结果，select输出gdb_sample_josh 和out共同的结果
# print(g.V('gdb_sample_josh').out('gdb_sample_created').aggregate('x').by('name').cap('x').toList())

# unfold使层级变少一级
# print(g.V('gdb_sample_josh').out('gdb_sample_created').aggregate('x').by('name').cap('x').unfold().toList())

# fold使层级变多一级
# print(g.V('gdb_sample_josh').out().values('name').fold().toList())

# 模式1："a"对应当前顶点，且创建了软件"lop", 模式2："b"对应软件名字叫"lop"， 模式3："c"对应创建软件"lop"的年龄为29的gdb_sample_person顶点
# print(g.V().match(as_('a').out('gdb_sample_created').has('name', 'lop').as_('b')
#                   ,as_('b').in_('gdb_sample_created').has('age', 29).as_('c'))
#                     .select('a','c').by('name').toList())

# sample随机进行采样
# print(g.V().hasLabel('gdb_sample_person').sample(2).by('age').toList())

# 每个顶点按照0.8的概率保留
# print(g.V().coin(0.8).toList())

# 对于label为gdb_sample_person的输出name，其他的输出常数"inhuman"
# print(g.V().coalesce(hasLabel('gdb_sample_person').values('name'),constant('inhuman')).toList())

# print(g.V('gdb_sample_lop').in_('gdb_sample_created').values('name').inject('Tom').map(length()).toList())

# 输出创建gdb_sample_lop的人的name
# print(g.V('gdb_sample_lop').in_('gdb_sample_created').map(values('name')).toList())

# 有问题
# print(g.V('gdb_sample_lop').in_('gdb_sample_created').map(lambda x: x.get().value("name")).toList())


# 先获取顶点"gdb_sample_lop"的入"created"顶点，再将每个顶点转化为出边(多条)
# print(g.V('gdb_sample_lop').in_('gdb_sample_created').flatMap(outE()).toList())

# barrier(): 在某个位置插入一个栅栏，以强制该位置之前的步骤必须都执行完成才可以继续往后执行
# 除了显示插入barrier外， order(), sample(), dedup(), aggregate(), fold(),
# count(), sum(), max(), min(), group(), groupCount(), cap()等会隐式的插入barrier

# local 很多操作是针对传递过来的对象流中的全部对象进行操作，但也有很多时候需要针对对象流中的单个对象而非对象流中的全部对象进行一些操作。
# 这种对单个对象的局部操作，可以使用local()语句实现。
# 暂时还没搞懂local的作用
# print(g.V().both().barrier().local(groupCount().by("name")).toList())

# 判断顶点"gdb_sample_josh"是否包含"gdb_sample_created"出顶点, hasNext()返回为True or False
# print(g.V('gdb_sample_josh').out('gdb_sample_created').hasNext())

# g.V('gdb_sample_josh').out('gdb_sample_created')返回的是一个遍历器(迭代器)，每次执行这句话实际上都是获取的迭代器的第一个元素，
# 若迭代器的返回结果为空，会报错，可先使用hasNext判断存不存在。
# iter = g.V('gdb_sample_josh').out('gdb_sample_created')
# print(iter.next())

# 获取顶点"gdb_sample_josh"的"gdb_sample_created"出顶点集合的前两个, next()与next(n)使用中有一点小小的区别，就是当没有元素或者没有足够多的元素时，执行next()会报错，但是执行next(n)则是返回一个空集合(List)。
# print(g.V('gdb_sample_josh').out('gdb_sample_created').next(2))
