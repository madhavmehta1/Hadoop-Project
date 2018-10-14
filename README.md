# MutualFriends-Hadoop
This Hadoop based project was submitted a part of course work CS6360 at UTD.

CS6350 
Big data Management Analytics and Management
Fall 2018
Homework 1   
Submission Deadline: 1th October, 2018

In this homework, you will be using hadoop/mapreduce to analyze social network data.

Q1

Write a MapReduce program in Hadoop that implements a simple “Mutual/Common friend list of two friends". The key idea is that if two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy


As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output). 


Input:
Input files 
1. soc-LiveJournal1Adj.txt 

The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
2. userdata.txt
The userdata.txt contains dummy data which consist of 
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.



Here, <User> is a unique integer ID corresponding to a unique user and <Friends> is a comma-separated list of unique IDs corresponding to the friends of the user with the unique ID <User>. Note that the friendships are mutual (i.e., edges are undirected): if A is friend with B then B is also friend with A. The data provided is consistent with that rule as there is an explicit entry for each side of each edge. So when you make the pair, always consider (A, B) or (B, A) for user A and B but not both.

Output: The output should contain one line per user in the following format:
<User_A>, <User_B><TAB><Mutual/Common Friend List>

where <User_A> & <User_B> are unique IDs corresponding to a user A and B (A and B are friend). < Mutual/Common Friend List > is a comma-separated list of unique IDs corresponding to mutual friend list of User A and B.

Please find the output for the following pairs:
(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)


Q2.
Please answer this question by using dataset from Q1.
Find friend pairs whose number of common friends (number of mutual friend) is within the top-10 in all the pairs. Please
output them in decreasing order.
Output Format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>

Q3.
Please use in-memory join to answer this question.
Given any two Users (they are friend) as input, output the list of the names and the states of their mutual friends.

Note: use the userdata.txt to get the extra user information.
Output format:
UserA id, UserB id, list of [states] of their mutual Friends.

Sample Output:
26, 28	[Evangeline: Ohio, Charlotte: California]

Results:
Question1: Output:
Mutual friends
{csgrads1:~/q1_attempt25} cat part-r-00000
0,1	5,20
1,29826	
20,28193	1
28041,28056	6245,28054,28061
6222,19272	19263,19280,19281,19282

Question 2:
Top 10 paris of friends who have the highest number of mutual friends
{csgrads1:~/output_q2_attempt2} cat part-r-00000
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
Last name and States of Mutual friends of user 28041 and 28056
28041,28056	[Stephanie:Louisiana,Carl:Tennessee,Taylor:Pennsylvania]

Question 4:
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
