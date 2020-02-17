/* grounds.sql
SQL script for building the grounds table. Further fields may be added over time
*/

create table grounds (groundid int primary key
, groundName varchar
, otherNames varchar
, groundLocation varchar
, Hometeam varchar
, address varchar
, Established int
, Capacity int
, Floodlights varchar
, OtherSports varchar
, Notes varchar
, postcode varchar
)