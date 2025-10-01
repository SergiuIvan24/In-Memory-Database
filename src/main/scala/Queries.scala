object Queries {
  def query_1(db: Database, ageLimit: Int, cities: List[String]): Option[Table] = {
    def find_table() : Option[Table] = {
      db.tables.find(_.tableName.equals("Customers"))
    }
    def apply_filters(table : Table) : Table = {
      table.filter(
        Field("age", age => age.toInt > ageLimit) &&
          Field("city", city => cities.contains(city))
      )
    }
    find_table().map(apply_filters)
  }

  def query_2(db: Database, date: String, employeeID: Int): Option[Table] = {
    def find_table() : Option[Table] = {
      db.tables.find(_.tableName.equals("Orders"))
    }
    def date_list(date : String) : List[Int] = date.split('-').toList.map(elem => elem.toInt)
    def compare_dates(date1 : String, date2 : String) : Int = {
      if(date_list(date1).head > date_list(date2).head)
        1
      else if(date_list(date1).head < date_list(date2).head)
        -1
      else {
        if(date_list(date1)(1) > date_list(date2)(1))
          1
        else if(date_list(date1)(1) < date_list(date2)(1))
          -1
        else {
          if(date_list(date1)(2) > date_list(date2)(2))
            1
          else -1
        }
      }
    }
    def apply_filters(table: Table) : Table = {
      table.filter(
        Field("date", data => compare_dates(data, date) == 1) &&
      Field("employee_id", employee_id => employee_id.toInt != employeeID)
      )
    }

    (find_table().map(apply_filters)) match
      case Some(value) => Some(value.select(List("order_id", "cost")).sort("cost", false))
      case _ => None
  }

  def query_3(db: Database, minCost: Int): Option[Table] = {
    def customersTable() : Table = db.selectTables(List("Customers")).get.apply(0)
    def ordersTable() : Table = db.selectTables(List("Orders")).get.apply(0)
    def tabela() : Table = {
      customersTable().join(ordersTable())("id", "customer_id")
        .filter(Field("cost", cost => cost.toInt > minCost))
        .select(List("order_id", "employee_id", "cost", "CustomerName"))
        .sort("employee_id", true)
    }
    Some(tabela())
  }
}
