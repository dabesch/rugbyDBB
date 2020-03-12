/* team_match_stats.sql
SQL script for building the team_match_stats table.
The columns are ordered: match info, scores, kicks/passes/runs, attacking, defensive, set pieces and discipline.
*/
create table team_match_stats (match_id int
,team varchar
,home int
,tries int
,conversion_attempts int
,conversion_goals int
,penalty_attempts int
,penalty_goals int
,Dropped_goals int
,Kick_at_goal_success float
,Kicks_from_hand int
,Passes int
,Runs int
,Metres_run_with_ball int
,possession_overall float
,possession_1H float
,possession_2H float
,territory_overall float
,territory_1H float
,territory_2H float
,Clean_breaks int
,Defenders_beaten int
,Offloads int
,rucks int
,rucks_won int
,mauls int
,mauls_won int
,Turnovers_conceded int
,tackles_made int
,tackles_missed int
,scrums int
,scrums_won int
,lineouts int
,lineouts_won int
,penalties_conceded int
,yellow_cards int
,red_cards int
)