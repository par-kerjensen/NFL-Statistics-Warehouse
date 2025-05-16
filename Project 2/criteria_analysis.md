Criteria Analysis

1. All 8 tables are from 4 different kaggle sources.
2. 2024_weekly_stats is from a pdf file, which is unstructured.
3. Some examples of logical entities are players, teams, stadiums, player stats, team stats, and etc..
4. Values of all locations and teams are consisted. Player stats and teams are consistent, for example, all measurements are measured using the standard yards for football.
5. superbowl_ratings.date stores a date value as a string.
6. team.team_division table stores null values as NULL.
7. stadiums.stadium_weather_station_name contains name, states, and country all in the same field.
8. spreadspoke_scores.weather_detail lists different weather conditions in the same cell.
9. stadiums.stadium_name and spreadspoke_scores.stadium use 2 different identifier systems to refer to the same identity.
10. The 2024_player_prediction table has information on players, their teams, and their stats. Fields like team and college shouldn't exist in the table because they are redundant, since these values would be stored repeatedly.
