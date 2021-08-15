/*

implementation of 'Singhal–Kshemkalyani vector clock

*/
#include <iostream>
#include<sstream>     //for stringstream function
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>      //
#include <sys/socket.h>     //      These libraries are used
#include <netinet/in.h>     //      for Socket prog.
#include <arpa/inet.h>      //

#include <thread>           //      This is the thread library
#include <random>           //      For generating exponential distribution
#include <chrono>           //      For measuring time in milliseconds
#include <mutex>            //      For mutex locks
#include <ctime>            //      For measuring the current system's time
#include <vector>           //      STL library for vectors
#include <fstream>          //      For reading/writing files

using namespace std;

ofstream file1;    //for writing the logs

int n, lambda, m, x;   //inputs declaration
double alpha;   

vector <int> *g;       //vector containing the graph toplogy

std::mutex mtx, mtx_wait;   //lock declaration

int internal_events, send_events, total_events;    
int total_send;
int space=0;   //for storing the space ultised in sending the msg

int wait = 0;   //for waiting for all TCP connection establishment

int total_tuples = 0;   //to calculate how many tuples send

//to generate delay ie exponentially distributed
double run_exp(float lambda) {
    default_random_engine generate;
    exponential_distribution <double> distribution(1.0/lambda);
    return distribution(generate);
}

void sender(int id, int *vector_clock, int *LS, int *LU) {
  /*
    Sender function first decides which nodes to connect random node to it's neighbours.
    After the connection is established, it sends the msg
    */

    
    int portno[g[id].size()];
    for(int i=0; i<g[id].size(); i++)
        portno[i]=(2000 + g[id][i]);
//port no. list to keep track of it's neigbour

    int sockfd[g[id].size()], n_tmp;//socket array for sending the message
    char *message;


    // Calculating probability of internal event
    double prob_int_event = (double)internal_events/(double)(internal_events + send_events);
    
    int ie = internal_events, se = send_events;
    
    // waiting until all TCP connection established
    while(wait < n);

    for(int i=0; i<g[id].size(); i++) {
        struct sockaddr_in serv_addr;
        sockfd[i] = socket(AF_INET, SOCK_STREAM, 0);   // We are using TCP socket
        
        if(sockfd[i] < 0) {
            perror("Error opening Socket.");
            exit(1);
        }

        memset(&serv_addr, '0', sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        
        int portNum = portno[i];
        serv_addr.sin_port = htons(portNum);

        if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
	        perror("Invalid address/Address not supported.");
	        exit(1);
	    }

        if(connect(sockfd[i], (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            perror("Connection falied.");
            exit(1);
        }
        //attaching socket with its port
    }

    for(int i=0; i<total_events; i++) {
        if(ie > 0 && se > 0) {
            srand(time(NULL));
            double r = ((double) rand() / (RAND_MAX));
            //to decide using rand() to do internal or sending event
            if(r >= prob_int_event) {
                // Internal event happens
                ie--;

                mtx.lock();

                time_t t = time(0);
                tm *now = localtime(&t);
                
                // updating it's logical time 
                vector_clock[id] += 1;
                // Incrementing the LU vector
                LU[id] += 1;
        //writing the internal event in log file
                file1 << "Process" << id + 1 << " excecutes internal event e" << id + 1 << i + 1 << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                for(int i=0; i<n; i++)
                    {
                    if(i==n-1)
                    file1 << vector_clock[i] << "]\n";
                    else
                    file1 << vector_clock[i] << " ";
                }
                
                mtx.unlock();
            } else {
                se--;

                // The random number is to be selected from 1 to g[id].size()
                srand(time(NULL));
                int rndNum = (rand() % g[id].size());

                int th_id = portno[rndNum] - 1999;

                mtx.lock();

                time_t t = time(0);
                tm *now = localtime(&t);

                // Increment its clock before sending
                vector_clock[id] += 1;
                // Incrementing the LU vector
                LU[id] += 1;

                
                string str="";

                /* comparing LSi[rndNUm] with LUi[k] to extract updated entries */
                for(int k=0; k<n; k++) {
                    if(LS[rndNum] < LU[k]) {
                         total_tuples++;
                        str += ("("+to_string(k) + " " + to_string(vector_clock[k])+")");
                    }
                }
                
                // Concatenatenating the id and msg number
                str += (", " + to_string(id + 1) + ", " + to_string(i + 1) + ";");

                // Sample msg sent : (1 2)(2 3), 2, 1;...

                message = (char *)str.c_str();
                space+=strlen(message)*sizeof(char);
                n_tmp = write(sockfd[rndNum], message, strlen(message));
                if(n_tmp < 0) {
                    perror("Error on writing.");
                    exit(1);
                }

                // Updating the LS entry(rndNum)
                LS[rndNum] = vector_clock[id];

                // writing event in log file
                file1 << "Process" << id + 1 << " sends message m" << id + 1 << i + 1 << " to process" << th_id << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                for(int i=0; i<n; i++)
                   {
                   if(i==n-1)
                   file1 <<  vector_clock[i] << "]\n";
                   else
                    file1 << vector_clock[i] << " ";
                   }
                

                mtx.unlock();
            }
        } else {
            if(ie == 0) {
                // Only send event happens
                se--;
                
                // Select a process from adjacency list on random
                
                // The random number is to be selected from 1 to g[id].size()
                srand(time(NULL));
                int rndNum = (rand() % g[id].size());

                // th_id is the thread id to which the msg is sent
                int th_id = portno[rndNum] - 1999;

                mtx.lock();

                time_t t = time(0);  //system time 
                tm *now = localtime(&t);

                // updating it's logical clock
                vector_clock[id] += 1;
                // Increment the LU vector
                LU[id] += 1;

                string str = "";

                /* comparing LSi[rndNUm] with LUi[k] to extract updated entries */
                for(int k=0; k<n; k++) {
                    if(LS[rndNum] < LU[k]) {
                        total_tuples++;
                        str += ("("+to_string(k) + " " + to_string(vector_clock[k])+")");
                    }
                    
                }

                str += (", " + to_string(id + 1) + ", " + to_string(i + 1) + ";");

   

                message = (char *)str.c_str();
                space+=strlen(message)*sizeof(char);
                // printf("msg = %s\n", msg);
                n_tmp = write(sockfd[rndNum], message, strlen(message));
                if(n_tmp < 0) {
                    perror("Error on writing.");
                    exit(1);
                }

                // Update the LS vector (i.e. the rndNum'th index)
                LS[rndNum] = vector_clock[id];

                // writing event in log file
                file1 << "Process" << id + 1 << " sends message m" << id + 1 << i + 1 << " to process" << th_id << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                for(int i=0; i<n; i++)
                   {
                   if(i==n-1)
                   file1 <<  vector_clock[i] << "]\n";
                   else
                    file1 << vector_clock[i] << " ";
                   }
                
                mtx.unlock();
            } else {
                // Only internal event happens
                ie--;

                mtx.lock();

                time_t t = time(0);
                tm *now = localtime(&t);

                // updating it's logical time
                vector_clock[id] += 1;
                // Increment the LU vector
                LU[id] += 1;

                file1 << "Process" << id + 1 << " excecutes internal event e" << id + 1 << i + 1 << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                for(int i=0; i<n; i++)
                   {
                   if(i==n-1)
                   file1 <<  vector_clock[i] << "]\n";
                   else
                    file1 << vector_clock[i] << " ";
                   }
                
                mtx.unlock();
            }
        }
        
        // Generating delay that is exponentially distributed with inter-event time λ ms
        this_thread::sleep_for(chrono::milliseconds((int)run_exp(lambda)));
    }

    for(int i=0; i<g[id].size(); i++)
        close(sockfd[i]);
}

void receiver(int id, int *vector_clock, int *LU) {
    /*
    Receiver creates the socket and listens for connections
    When a connection is estabilished
    */
    int total_connections = 0, flag = 1;
    char buffer[2000];

    for(int i=0; i<n; i++) {
        for(int j=0; j<g[i].size(); j++) {
            if(g[i][j] == id) {
                 total_connections++;
            }
        }
    }
//socket definition
    int socket_id, newsockfd[total_connections], portno, c, temp1,temp_socket;

    struct sockaddr_in server_address, client_address;
    socklen_t clientlength;

    // Create the socket
    socket_id = socket(AF_INET, SOCK_STREAM, 0);   // TCP socket is used here
    if(socket_id < 0) {
        perror("Error opening Socket.");
        exit(1);
    }
  //clearing the server addresss
    memset(&server_address,'0',sizeof(server_address));
    portno = 2000 + id;

    if (setsockopt(socket_id, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &flag, sizeof(flag))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    // Preparing the sockaddr_in structure
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(portno);

    // Binding the socket
    if(bind(socket_id, (struct sockaddr *) &server_address, sizeof(server_address)) < 0) {

        perror("Binding Failed.");
        exit(1);
    }

    // Socket listening
    listen(socket_id,total_connections);  /// total_connections = Max threads that can interact with other threads

    mtx_wait.lock();
    wait++;
    mtx_wait.unlock();

    
    clientlength = sizeof(client_address);

    int client_connection = 0;
    // Accept incoming connections
    while(client_connection < total_connections) {
        if(temp_socket = accept(socket_id, (struct sockaddr *) &client_address, &clientlength)) {
            newsockfd[client_connection] = temp_socket;
            client_connection++;
        }
    }

     while(total_send) {
        for(int i=0; i<total_connections; i++) {
            bzero(buffer, 2000);
            temp1 = read(newsockfd[i], buffer, 2000);
            if(temp1 < 0) {
                perror("Error on reading.");
                exit(1);
            }
            if(strlen(buffer) > 0) {
                mtx.lock();

       
                // buffer : "(1 2)(1 3), 3, 2;..."
                int ind = 0;
                while(buffer[ind] != '\0') {
                    char c;
                    vector<int> indexes, vals;
                    int var, thread_send, msg_send;
                    // Parsing till the ','
                    while(buffer[ind] != ',') {
                        var = 0;
                        while(buffer[ind] != ')') {
                            if(buffer[ind] != ' ' && buffer[ind] != '(') {
                                c = buffer[ind];
                                var = 10 * var + c - 48;
                                if(buffer[ind + 1] == ')') {
                                    vals.push_back(var);
                                    var = 0;
                                }
                            } else if(buffer[ind] == ' ') {
                                indexes.push_back(var);
                                var = 0;
                            }
                            ind++;
                        }
                        ind++;
                    }

                    ind += 2;
                    var = 0;
                    // Parsing the thread which sent this msg
                    
                    while(buffer[ind] != ',') {
                        c = buffer[ind];
                        var = 10 * var + c - 48;
                        ind++;
                    }
                    thread_send = var;

                    ind += 2;
                    var = 0;
                    // Parsing the msg number
                    while(buffer[ind] != ';') {
                        c = buffer[ind];
                        var = 10 * var + c - 48;
                        ind++;
                    }
                    msg_send = var;

                    ind++;

                    time_t t = time(0);  //system time
                    tm *now = localtime(&t);

                    vector_clock[id] += 1;
                    // Increment the LU vector
                    LU[id] += 1;

                    //updating it's vector clock according to tuples send by the sender
                    for(int k=0; k<indexes.size(); k++) {
                        vector_clock[indexes[k]] = max(vector_clock[indexes[k]], vals[k]);
                        LU[indexes[k]] = vector_clock[id];
                    }
         
          //writing the event in log file
                    file1 << "Process" << id + 1 << " receives m" << thread_send << msg_send << " from process" << thread_send << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                    for(int i=0; i<n; i++)
                        {
                        if(i==n-1)
                        file1 <<  vector_clock[i] << "], tuples: {";
                        else
                        file1 << vector_clock[i] << " ";
                        }
                    
                    for(int k=0; k<indexes.size()-1; k++)
                        file1 << "(" << indexes[k] + 1 << " " << vals[k] << "), ";
                    file1 << "(" << indexes[indexes.size() - 1] + 1 << " " <<vals[indexes.size() - 1] << ")}\n";

                    total_send--;

                }

                mtx.unlock();
            }
        }
    }
}

void fun(int id) {
    // This function creates 2 threads : sender and receiver
    

    // vector_clock is the vector clock of each process
    int *vector_clock;
    vector_clock = new int[n];

    // LS[] is the Last sent vector
    // LU[] is the Last updated vector
    int *LS, *LU;
    LS = new int[n];
    LU = new int[n];
    
    // All clocks,LS and LU are intialised with 0
    for(int i=0; i<n; i++)  {
        vector_clock[i] = 0;
        LS[i] = 0;
        LU[i] = 0;
    }
    //sender and receiver thread definition
    thread rev(receiver, id, vector_clock, LU);
    thread sn(sender, id, vector_clock, LS, LU);
   //threads are joined after executing the work
    sn.join();
    rev.join();
}

int main()
{
    string temp;
    
    fstream file;
    file.open("inp-params.txt");

    // Parsing of the input file
    int i=0,num;
    
    while(getline(file, temp))
    {
        //cout<<temp<<endl;
        stringstream s(temp);  //stream of characters to read from file
        string word;
        if(i==0)
        {
            s>>n>>lambda>>alpha>>m;
            i++;
            g=new vector<int>[n];  //to store graph topology

        } 
   
        else
        {
        	int j=0;
        	while(s>>word)
        	{
        	        //cout<<word<<" "<<j<<endl;
        		if(j>0)
        		{
         			g[num-1].push_back(stoi(word)-1);
        		}
        		else
        		{
        			num=stoi(word);
        			j++;
        		}	
        	}
        }
        s.clear();
        
    }
    internal_events = alpha * m;
    send_events = m;
    total_events = internal_events + send_events;

    total_send = send_events * n;

     printf("internal events per thread= %d, send events per thread= %d, total events per thread= %d\n",    internal_events, send_events, total_events);
    file.close();

   

    // Each node of the graph creates its own thread
    thread th[n];

    file1.open("Log_SK.txt");
   
    file1<<"internal events per thread= "<<internal_events<<"\nsend events per thread= "<<send_events<<"\ntotal Events per thread= "<<total_events<<"\ntotal messages sent per thread= "<<m*n;
    file1<<"\nspace for storing vector clock and LS and LU's vector used by each thread= "<<3*n*sizeof(int)<<" bytes"<<endl;
    
    for(int i=0; i<n; i++) {
        th[i] = thread(fun, i); 
    }

    // Threads are joined after completing all the events
    for(int i=0; i<n; i++) {
        th[i].join();
    }

    file1.close();

    printf("total tuples sent = %d\n", total_tuples);
    cout<<"space for storing vector clock including LS and LU's vector used by each thread= "<<3*n*sizeof(int)<<" bytes"<<endl;
     cout<<"space for sending the message used by each thread= "<<space/n<<" bytes"<<endl;

    return 0;
}
