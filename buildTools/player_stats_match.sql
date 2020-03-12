/* player_stats_match.sql
SQL script for building the player_stats_match table.
Shows detailed peformance stats for each player in a game
*/
-- todo: add playerID reference
create table player_stats_match (Pos varchar
,initName varchar
,points int
,distance_run int
,clean_breaks int
,defenders_beaten int
,offloads int
,turn_overs int
,penalties_conceded int
,match_id int
,nation varchar
,tries int
,try_assists int
,kicks int
,passes int
,runs int
,tackles_made int
,tackles_missed int
,lineouts_won int
,lineouts_stolen int
,yellows int
,reds int
)