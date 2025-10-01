import scala.collection.mutable.{ListBuffer, Set => MutableSet}
import scala.collection.immutable.ListMap // Import ListMap
type Row = Map[String, String]
type Tabular = List[Row]

case class Table (tableName: String, tableData: Tabular) {

  // TODO 1.0
  def header: List[String] = tableData.head.keys.toList
  def data: Tabular = tableData
  def name: String = tableName


  // TODO 1.1
  override def toString: String = {
    val hdr = this.header.mkString(",")
    val data = this.tableData.map(line => line.values.mkString(",")).mkString("\n")
    hdr + "\n" + data
  }

  // TODO 1.3
  def insert(row: Row): Table = {
    if(this.tableData.contains(row))
      new Table(tableName, tableData)
    else new Table(tableName, tableData.appended(row))
  }

  // TODO 1.4
  def delete(row: Row): Table = {
    if(this.tableData.contains(row))
      val newTableData : Tabular = tableData.filter(elem => elem != row)
      new Table(tableName, newTableData)
    else new Table(tableName, tableData)
  }

  // TODO 1.5
  def sort(column: String, ascending: Boolean = true): Table = {
    if(this.header.contains(column)) {
      val newData : Tabular = this.data.sortBy(row => row.get(column))
      if(ascending)
        new Table(tableName, newData)
      else new Table(tableName, newData.reverse)
    } else new Table(tableName, tableData)
  }

  // TODO 1.6
  def select(columns: List[String]): Table = {
    val newData : Tabular = this.tableData.map(row => {
      row.filter((key, value) => columns.contains(key))
    })
    new Table(tableName, newData)
  }

  // TODO 1.7
  def cartesianProduct(otherTable: Table): Table = {
    val newData : Tabular = {
      for row1 <- this.tableData yield {
        for row2 <- otherTable.tableData yield {
          val randPrefix1 = row1.map((key, value) => (this.tableName + '.' + key, value))
          val randPrefix2 = row2.map((key, value) => (otherTable.tableName + '.' + key, value))
          randPrefix1 ++ randPrefix2
        }
      }
    }.flatten
      new Table(this.tableName + 'X' + otherTable.tableName, newData)
  }

  // TODO 1.8
  def join(other: Table)(col1: String, col2: String): Table = {
    if(this.tableData.isEmpty) return other
    if(other.tableData.isEmpty) return this

    def mergeValues(s1 : String, s2 : String) : String = {
      (s1, s2) match
        case ("", s2) => s2
        case (s1, "") => s1
        case (s1, s2) => {
          if(s1.equals(s2)) s1
          else s1 + ';' + s2
        }
    }

    val allColumns = (this.header.filter(elem => elem != col1) ++ other.header.filter(elem => elem != col2)).distinct
    val matchingRows : Tabular = for row1 <- this.tableData
      row2 <- other.tableData if(row1(col1) == row2(col2)) yield {
      val mergedRow = Map(col1 -> row1(col1))
      allColumns.foldLeft(mergedRow){(acc, col) =>
        val val1 = row1.getOrElse(col, "")
        val val2 = row2.getOrElse(col, "")
        val mergedValue = mergeValues(val1, val2)
        acc + (col -> mergedValue)
      }
    }

    val onlyLeftRows : Tabular = for(row1 <- this.tableData
      if(!other.tableData.exists(row2 => row1(col1) == row2(col2)))) yield {
      val acc = Map(col1 -> row1(col1))
      allColumns.foldLeft(acc){(acc, col) =>
        if(row1.contains(col))
          acc + (col -> row1(col))
        else
          acc + (col -> "")
      }
    }

    val onlyRightRows : Tabular = for row2 <- other.tableData
      if(!this.tableData.exists(row1 => row1(col1) == row2(col2))) yield {
      val acc = Map(col1 -> row2(col2))
      allColumns.foldLeft(acc) {(acc, col) =>
        if(row2.contains(col))
          acc + (col -> row2(col))
        else acc + (col -> "")
      }
    }
    new Table("Join", matchingRows ++ onlyLeftRows ++ onlyRightRows)
  }

  // TODO 2.3
  def filter(f: FilterCond): Table = {
    val data1 = this.tableData.filter(row =>
      f.eval(row) match
        case Some(value) => value
        case None => false
    )
    new Table(this.tableName, data1)
  }

  // TODO 2.4
  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val data1 = this.tableData.map(row =>{
      if(f.eval(row) == Some(true)) {
        row ++ updates
      } else row
    })
    new Table(tableName, data1)
  }

  // TODO 3.5
  def apply(index : Int) : Row = {
    tableData(index)
  }
}

object Table {
  // TODO 1.2
  def fromCSV(csv: String): Table = {
    val rows = csv.split("\n").toList
    val hdr = rows.head.split(',').toList
    val date = rows.tail.map(row => row.split(',').toList)
    val tableData : Tabular = date.map(
      row => hdr.zip(row).toMap
    )
      new Table("Name", tableData)
  }

  // TODO 1.9
  def apply(name: String, s: String): Table = {
    val tableData : Tabular = fromCSV(s).tableData
    new Table(name, tableData)
  }
}