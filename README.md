# ndc-refill

first change DB
 - host
 - username
 - password

then change

 - flagcode
 - ship position starting id (should be much higher than the current shipposition id)

--------------------------------------------------

Run script

--------------------------------------------------

After
 - change shipposition sequence id in table to appropriate value


------------------------------------------------------------------

#Process
1) Set DO_INSERTS flag to false
2) update DB Values from Google Sheet
3) Set ShippositionID to be something reasonable
4) Find ship with missing positions and check in UI
5) Run project
3) Check ShippositionID is something reasonable
6) If all looks good, set Do inserts to true and Push go again
7) check ui for missing positions
8) Find and update generator value to something reasonable
   Update Generator
   set sequence_next_hi_value=
   where sequence_name='ShipPosition';
9) restart dc
   service jboss restart
10) save log file
    ***can roll back by deleting all positions with id above our ShippositionID variable 
