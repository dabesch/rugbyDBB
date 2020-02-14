/* playerStats.sql
SQL script for building the playerStats table. Uses a serial index.
*/

create table playerStats ( matchStatID SERIAL PRIMARY KEY,
matchID int,
groundID int,
playerID int,
pos varchar(2),
startGame int,
pts int,
tries int,
conv int,
pens int,
dropG int,
result varchar,
team varchar,
opposition varchar,
ground varchar,
matchDate timestamp,
matchlink varchar
);
