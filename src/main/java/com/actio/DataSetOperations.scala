package com.actio

object DataSetOperations {

  // goal here with merge left is that all elements on the left hand side work as usual
  // if there is clash of labels, the right hand side will be shifted until there is no longer a clash
  def mergeLeft(l: DataSet, r: DataSet): DataSet = mergeLeft(l, r, r.label)

  def mergeLeft(l: DataSet, r: DataSet, newLabel: String): DataSet = {

    if(r.elems.toList.length == 0)
      l
    else {
      val re = r.elems.toList.head
      val rest = r.elems.toList.tail

      val findlabel = l.elems.find(f => f.label == re.label)
      if(!findlabel.isDefined)
        mergeLeft(DataRecord(l.label, re :: l.elems.toList), DataRecord(r.label, rest), newLabel)
      else {
        if(findlabel.get.isInstanceOf[DataRecord] && re.isInstanceOf[DataRecord])
          mergeLeft(DataRecord(l.label, mergeLeft(findlabel.get, re, newLabel) :: l.elems.filterNot(f => f.label == re.label).toList), DataRecord(r.label, rest), newLabel)
        else
          mergeLeft(l, DataRecord(r.label, DataRecord(newLabel, List(re)) :: rest), newLabel)
        }
    }
  }
}
