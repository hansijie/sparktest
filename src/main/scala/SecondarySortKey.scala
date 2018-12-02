class SecondarySortKey(val first:Int,val second:Int) extends Ordered [SecondarySortKey] with Serializable {
def compare(other:SecondarySortKey):Int = {
    if (this.first - other.first !=0) {
         this.first - other.first 
    } else {
      this.second - other.second
    }
  }
}

