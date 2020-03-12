/* match_details_log.sql
SQL script for building the match_details_log table.
Functions as a logging resource to log what has been scraped from each match
*/
create table match_details_log (match_id int primary key
, notes_check int
, timeline_check int
, team_check int
, match_stats_check int
, team_stats_check int
)