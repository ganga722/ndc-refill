# ndc-refill

--------------------------------------------------

### Process
1) Set DO_INSERTS and UPDATE_GENERATOR flags to false
2) update DB Values from Google Sheet
3) Find ship with missing positions and check in UI
4) Run project
5) Check ShippositionID is something reasonable
6) If all looks good, set flags from (1) to true and Push go again
7) check ui for missing positions
8) Check logs to see that generator values have updated
9) restart dc
   service jboss restart
10) save log file
    ***can roll back by deleting all positions with id above our ShippositionID variable 
