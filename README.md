# Amazon CloudSearch Committer

Amazon CloudSearch implementation of Norconex Committer. 

Website: https://opensource.norconex.com/committers/cloudsearch/

## Compatibility Matrix

| This Committer   | Committer Core | HTTP Collector | FS Collector |
| ---------------- | -------------- | -------------- | ------------ |
| **1.x**          | 2.x            | 2.x            | 2.x          |
| **2.x** (master) | 3.x            | 3.x            | -            |


## Notes

* For all unit tests to run locally, please use the Nozoma CloudSearch
  project (https://hub.docker.com/r/oisinmulvihill/nozama-cloudsearch) 
  and set the following system property (shown here with 
  default location):
    
    -Dcloudsearch.endpoint="http://localhost:15808"
