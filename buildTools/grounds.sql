/* grounds.sql
SQL script for building the grounds table
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