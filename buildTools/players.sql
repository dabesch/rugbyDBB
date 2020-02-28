/* players.sql
SQL script for building the players table
*/

create table players (playerID int primary key
, name varchar
,Fullname varchar
, Firstname varchar
, Lastname varchar
,initName varchar
,Born timestamp
,Majorteams varchar
, Position varchar
, Height float
, Weight float
, Relations varchar
, RelationsJSON varchar
, Hometown varchar
, nation varchar
, Died timestamp
,placeOfDeath varchar
)