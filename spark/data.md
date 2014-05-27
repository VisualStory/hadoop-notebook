Data
====

#### Create Spark Context

```scala
object Data {
  def main(args: Array[String]]) {
    val sc = new SparkContext(args(0), "Data")
  }
}
```

- `args(0)` is the first parameter from command line

#### Load input file

```scala
val textFile = sc.textFile(args(1))
```

#### Read data from input file with delimiter

```scala
var data = textFile.map(line => line.split(",", -1)).cache
```

- `split(",", -1)`: `","` is delimiter, `-1` means missing values are not ignored

#### Get header

```scala
val header = data.first
```

#### Remove header from data

```scala
data = data.mapPartitionsWithIndex{ case (index, iter) =>
  if (index == 0) iter.drop(1) else iter
}
```

- `mapPartitionsWithIndex(f: (Int, Iterator[T]) => Iterator[U])`: Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition.

#### Process by column

```scala
val length = data.first.length
for (i <- 0 until length) {
  process(data.map(_(i)))
}
```

- `data.map(_(i))`: returns a RDD by selecting only a certain column of index `i`

#### Print data

```scala
  def printData(data: RDD[Array[String]], info: String) {
    println(info + " (Number of features: " + data.first.length + ")")
    println("=" * info.length)
    data.foreach(x => println(x.mkString("|", "\t", "\t|")))
    println
  }
```
