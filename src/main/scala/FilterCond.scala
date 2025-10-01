import scala.language.implicitConversions

trait FilterCond {
  def eval(r: Row): Option[Boolean]

  // TODO 2.2
  def ===(other: FilterCond): FilterCond = Equal(this, other)
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  def unary_! : FilterCond = Not(this)
}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  override def eval(r: Row): Option[Boolean] =  {
    r get (colName) match
      case Some(value) => Some(predicate(value))
      case None => None
  }
}

case class Compound(op: (Boolean, Boolean) => Boolean, conditions: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    val val1 = conditions.map(conditie => conditie.eval(r))
    val primaVal = val1.head
    val1.foldLeft(primaVal)((acc, operatie) => Some(op(acc.get, operatie.get)))
  }
}

case class Not(f: FilterCond) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    f.eval(r) match
      case Some(true) => Some(false)
      case Some(false) => Some(true)
      case None => None
  }
}

def And(f1: FilterCond, f2: FilterCond): FilterCond = {
  val list = List(f1, f2)
  Compound((a, b) => a && b, list)
}
def Or(f1: FilterCond, f2: FilterCond): FilterCond = {
  val list = List(f1, f2)
  Compound((a, b) => a || b, list)
}
def Equal(f1: FilterCond, f2: FilterCond): FilterCond = {
  val list = List(f1, f2)
  Compound((a, b) => a == b, list)
}

case class Any(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    fs.foldLeft(Some(false) : Option[Boolean])((acc, op) => {
      (acc, op.eval(r)) match
        case (Some(a), Some(b)) => Some(a || b)
        case _ => None
    })
  }
}

case class All(fs: List[FilterCond]) extends FilterCond {
  override def eval(r: Row): Option[Boolean] = {
    fs.foldLeft(Some(true) : Option[Boolean])((acc, op) => {
      (acc, op.eval(r)) match
        case (Some(a), Some(b)) => Some(a && b)
        case _ => None
    })
  }
}