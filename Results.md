Results:

Question1:

hadoop jar MutualFriends.jar MutualFriends <soc-LiveJournal1Adj.txt> <output_path>

Output: Mutual friends {csgrads1:~/q1_attempt25} cat part-r-00000

0,1	5,20

1,29826

20,28193	1

28041,28056	6245,28054,28061

6222,19272	19263,19280,19281,19282

Question 2:

hadoop jar Top10CommonFriends.jar Top10CommonFriends <soc-LiveJournal1Adj.txt> <output_path>

Top 10 paris of friends who have the highest number of mutual friends {csgrads1:~/output_q2_attempt2} cat part-r-00000

18683,18728	99

18688,18728	99

18698,18699	99

18685,18696	99

18683,18710	99

18676,18721	99

18681,18707	99

18695,18711	99

18676,18712	99

18722,18729	99

Question 3:

hadoop jar StatesOfMutualFriends.jar StatesOfMutualFriends <soc-LiveJournal1Adj.txt> <userdata.txt> <temp_path> <output_path>

Last name and States of Mutual friends of user 28041 and 28056

28041,28056	[Stephanie:Louisiana,Carl:Tennessee,Taylor:Pennsylvania]

Question 4:

hadoop jar MinAge.jar MinAge <soc-LiveJournal1Adj.txt> <userdata.txt> <temp_path1> <temp_path2> <output_path>

Angela,Roybal,1656 Marietta Street,Santa Rosa,California	88

Lewis,Dawson,4833 Stewart Street,Indianapolis,Indiana	88

Sara,Carter,3301 Palmer Road,Westerville,Ohio	88

Lottie,Tucker,4035 Canis Heights Drive,Los Angeles,California	88

Patricia,Countryman,2661 Counts Lane,Cincinnati,Kentucky	88

Tyson,Witkowski,4181 Richards Avenue,Stockton,California	88

Richard,Wilke,4287 Locust Street,Albany,Georgia	88

Mary,Kennedy,1833 Boring Lane,San Francisco,California	88

Maureen,Creasey,568 Columbia Road,Denver,Colorado	88

Randee,Raymond,238 Golden Street,Miami,Florida	88
