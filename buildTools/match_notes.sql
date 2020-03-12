/* match_notes.sql
SQL script for building the match_notes table.
Overall summary of the game where available.
*/
create table match_notes (match_id int primary key
, result varchar
, attendance int
, ground_name varchar
, ground_name_id int
, referee varchar
, referee_id int
)