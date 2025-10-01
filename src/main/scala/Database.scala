case class Database(tables: List[Table]) {
  // TODO 3.0
  override def toString: String = {
    tables.map(elem => elem.toString).toString()
  }

  // TODO 3.1
  def insert(tableName: String): Database = {
    if(tables.exists(table => table.tableName.equals(tableName)))
      this
    else {
      val new_table = new Table(tableName, Nil)
      val new_tables = tables.appended(new_table)
      Database(new_tables)
    }
  }

  // TODO 3.2
  def update(tableName: String, newTable: Table): Database = {
    if(!tables.exists(_.tableName.equals(tableName)))
      this
    else {
      val new_tables = tables.map(table =>
      if(table.tableName.equals(tableName))
        newTable
      else
        table
      )
      new Database(new_tables)
    }
  }
  // TODO 3.3
  def delete(tableName: String): Database = {
    if(!tables.exists(_.tableName.equals(tableName)))
      this
    else {
      val new_tables = tables.filter(elem => !elem.tableName.equals(tableName))
      new Database(new_tables)
    }
  }

  // TODO 3.4
  def selectTables(tableNames: List[String]): Option[Database] = {
    val selectedTables = tables.filter(elem => tableNames.contains(elem.tableName))
    if(selectedTables.length != tableNames.length)
      None
    else Some(Database(selectedTables))
  }

  // TODO 3.5
  def apply(index : Int) : Table = {
    this.tables(index)
  }
}
