package com.actio.dpsystem

/**
  * Created by jim on 25/07/16.
  */

// run system pipelines for publishing
class DPEventPublisher(val dprun: DPSystemRuntime) {

  def pushConfig(): Unit = {

    // treat config as a datasource - return a dataset

    /*



     */

  }

  def getRun(): Unit = {


    // treat config as a datasource - return a dataset

    /*
        Given a pipeline

        1. Create Run
          - Config Name : System -> configName
          - RunID

        2. Create Pipeline
          - Pipeline Name : Nodes -> Pipeline
          - RUNID + PIPEUUID

        3. Create Taask
        - Task Name : Nodes ->
        - RUNID + PIPEUUID + TASKUUID


     */


  }

  def pushEvents(): Unit = {

  }

}

class ConfigDataSource {


}