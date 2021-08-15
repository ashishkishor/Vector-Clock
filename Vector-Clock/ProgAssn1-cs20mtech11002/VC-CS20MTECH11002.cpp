#include <iostream>
#include<sstream>     //for stringstream
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

ofstream file1;   //logFile for ouput

int n, lambda, m, x;   //inputs
double alpha;
int space=0;          //to store space in each message while sending
vector <int> *g;

mutex mtx, mtx_wait;               //locks intialization

int internal_events, send_events, total_events;
int total_send;

//for waiting to establish all TCP connnections before sending any msg
int wait = 0;      

//for generating delay ie exponentially distributed
double run_exp(float lambda) {
    default_random_engine generate;
    exponential_distribution <double> distribution(1.0/lambda);
    return distribution(generate);
}

void receiver(int,int*);  //function definitions of receiver and sender
void sender(int,int*);
void fun(int id) {
    // This function creates 2 threads : sender and receiver

    // vector_clocl[] is the vector clock for each process
    int *vector_clock;
    vector_clock = new int[n];

    // All clocks are intialised with 0
    for(int i=0; i<n; i++)  
        vector_clock[i] = 0;

    thread rev(receiver, id, vector_clock);
    thread sn(sender, id, vector_clock);

    // printf("Threads created\n");

    sn.join();
    rev.join();
}

void receiver(int id, int *vector_clock) {
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

    int socket_id, newsockfd[total_connections], portno, c, temp1,temp_socket;
 
 //socket definition
    struct sockaddr_in server_address, client_address; 
    socklen_t clientlength;

    // Create the socket
    socket_id = socket(AF_INET, SOCK_STREAM, 0);   // using TCP socket
    if(socket_id < 0) {
        perror("Error opening Socket.");
        exit(1);
    }
   //clearing the serveraddress
    memset(&server_address, '0', sizeof(server_address));
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
    listen(socket_id, total_connections);  // total_connections = Max threads that can interact with other threads

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
            temp1 = read(newsockfd[i], buffer, 2000); //reading from buffer
            if(temp1 < 0) {
                perror("Error on reading.");
                exit(1);
            }
            if(strlen(buffer) > 0) {
                mtx.lock();

                // Extract the vector clock from buffer
                // Sample buffer : "1 2 3 4 , 3 , 10;"
                //3 is sender process id and 10 is the message no. 
                int index = 0;
                while(buffer[index] != '\0') {
                    char c;
                    vector<int> message_clock;
                    int temp = 0, recieved_process, recieved_message;

                    // Parsing till the ','
                    while(buffer[index] != ',') {
                        if(buffer[index] != ' ') {
                            c = buffer[index];
                            temp = 10 * temp + c - 48;
                        } else {
                            message_clock.push_back(temp);
                            temp = 0;
                        }
                        index++;
                    }

                    index += 2;
                    temp = 0;
                    // Parsing the thread which sent this msg
                    while(buffer[index] != ',') {
                        c = buffer[index];
                        temp = 10 * temp + c -48;
                        index++;
                    }
                    recieved_process = temp;

                    index += 2;
                    temp= 0;
                    // Parsing the msg number
                    while(buffer[index] != ';') {
                        c = buffer[index];
                        temp = 10 * temp + c -48;
                        index++;
                    }
                    recieved_message = temp;

                    index++;

                    time_t t = time(0); //system time
                    tm *now = localtime(&t);

                    // updating the reciever vector clock

                    for(int i=0; i<n; i++)
                        vector_clock[i] = max(vector_clock[i], message_clock[i]);
                    //updating its logical clock
                    vector_clock[id] += 1;
               //writing event on log file 
                    file1 << "Process" << id + 1 << " receives m" << recieved_process << recieved_message << " from process" << recieved_process << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                    for(int i=0; i<n; i++)
                        {
                        if(i==n-1)
                        file1<<vector_clock[i]<<"]\n";
                        else
                        file1 << vector_clock[i] << " ";
                        }
               
                    total_send--;

                }

                mtx.unlock();
            }
        }

        
    }
}
void sender(int id, int *vector_clock) {
    /*
    Sender function first decides which nodes to connect random node to it's neighbours.
    After the connection is established, it sends the msg
    */

    
    int portno[g[id].size()];
    for(int i=0; i<g[id].size(); i++)
        portno[i]=(2000 + g[id][i]);    
//port no. list to keep track of it's neigbour

    int sockfd[g[id].size()], n_tmp; //socket array for sending the message
    char *msg;


    // Calculating probability of internal event
    double prob_int_event = (double)internal_events/(double)(internal_events + send_events);

    int ie = internal_events, se = send_events;

    // waiting until all TCP connection established
    while(wait < n);

    for(int i=0; i<g[id].size(); i++) {
        struct sockaddr_in serv_addr;
        sockfd[i] = socket(AF_INET, SOCK_STREAM, 0);   // We are using TCP
       

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
         //attaching socket with port
        if(connect(sockfd[i], (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            perror("Connection falied.");
            exit(1);
        }
        // Connection is established.
    }

    for(int i=0; i<total_events; i++) {
        if(ie > 0 && se > 0) {
            srand(time(NULL));
            //to decide using rand() to do internal or sending event
            double r = ((double) rand() / (RAND_MAX));
            if(r >= prob_int_event) {
                // Internal event happens
                ie--;

                mtx.lock();

                time_t t = time(0);
                tm *now = localtime(&t);
                
                // updating it's logical time
                vector_clock[id] += 1;

                file1 << "Process" << id + 1 << " excecutes internal event e" << id + 1 << i + 1 << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                for(int i=0; i<n; i++)
                        {
                        if(i==n-1)
                        file1<<vector_clock[i]<<"]\n";
                        else
                        file1 << vector_clock[i] << " ";
                        }
                
                mtx.unlock();
            } else {
                se--;

    
                // The random number is to be selected from 1 to g[id].size()
                srand(time(NULL));
                int rndNum = (rand() % g[id].size());

                // th_id is the thread id to which the msg to send
                int th_id = portno[rndNum] - 1999;

                mtx.lock();

                time_t t = time(0);
                tm *now = localtime(&t);

                // Increment its clock before sending
                vector_clock[id] += 1;

                // Concatenating the buffer with vector clock
                string str="";
                for(int i=0; i<n; i++)
                    str += (to_string(vector_clock[i]) + " ");   
                // Concatenatenating the id and msg number
                str += (", " + to_string(id + 1) + ", " + to_string(i + 1) + ";");
                msg = (char *)str.c_str();
                space+=strlen(msg)*sizeof(char);
                n_tmp = write(sockfd[rndNum], msg, strlen(msg));
                if(n_tmp < 0) {
                    perror("Error on writing.");
                    exit(1);
                }

                // writing the event in log file
                file1 << "Process" << id + 1 << " sends message m" << id + 1 << i + 1 << " to process" << th_id << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                for(int i=0; i<n; i++)
                        {
                        if(i==n-1)
                        file1<<vector_clock[i]<<"]\n";
                        else
                        file1 << vector_clock[i] << " ";
                        }
                
                mtx.unlock();
            }
        } else {
            if(ie == 0) {
                // Only send event happens
                se--;
                
                
                // The random number is to be selected from 1 to g[id].size()
                srand(time(NULL));
                int rndNum = (rand() % g[id].size());

                // th_id is the thread id to which the msg is sent
                int th_id = portno[rndNum] - 1999;

                mtx.lock();

                // Print message sent in the format
                time_t t = time(0);
                tm *now = localtime(&t);

                // Increment its clock before sending
                vector_clock[id] += 1;

                // The msg sent is the vector clock 
                string str = "";
                for(int i=0; i<n; i++)
                    str += (to_string(vector_clock[i]) + " ");
                str+= (", " + to_string(id + 1) + ", " + to_string(i + 1) + ";");
                msg = (char *)str.c_str();
                space+=strlen(msg)*sizeof(char);
                n_tmp = write(sockfd[rndNum], msg, strlen(msg));
                if(n_tmp < 0) {
                    perror("Error on writing.");
                    exit(1);
                }

                file1 << "Process" << id + 1 << " sends message m" << id + 1 << i + 1 << " to process" << th_id << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                for(int i=0; i<n; i++)
                        {
                        if(i==n-1)
                        file1<<vector_clock[i]<<"]\n";
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

                // it's logical time updated
                vector_clock[id] += 1;

                file1<< "Process" << id + 1 << " excecutes internal event e" << id + 1 << i + 1 << " at " << now->tm_min << ":" << now->tm_sec << ", vc: [";
                for(int i=0; i<n; i++)
                        {
                        if(i==n-1)
                        file1<<vector_clock[i]<<"]\n";
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
        //closing the socket
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
        stringstream s(temp);   //to declare stream of words
        string word;
        if(i==0)
        {
            s>>n>>lambda>>alpha>>m;    //extracting the input from stream
            i++;
            g=new vector<int>[n];

        }                    
        //now reading the graph topology 
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

     printf("n = %d, lambda = %d, alpha = %f, m = %d\n", n, lambda, alpha, m);

    internal_events = alpha * m;
    send_events = m;
    total_events = internal_events + send_events;

    total_send = send_events * n;

     printf("internal events per thread= %d, send events per thread = %d, total events per thread= %d\n", internal_events, send_events, total_events);
     
     
     //cout<<"size= "<<sizeof(int)<<endl;
    file.close();
    // Parsing finished

    thread th[n];
    file1.open("log_VC.txt");
    file1<<"internal events per thread= "<<internal_events<<"\nsend events per thread= "<<send_events<<"\ntotal Events per thread= "<<total_events<<"\ntotal messages sent per thread= "<<m*n;
    file1<<"\nspace for storing vector clock used by each thread= "<<n*sizeof(int)<<" bytes"<<endl;
    
    for(int i=0; i<n; i++) {
        th[i] = thread(fun, i); 
    }

    // Threads are joined after completing all the events
    for(int i=0; i<n; i++) {
        th[i].join();
    }

    file1.close();
    printf("total messages sent by each thread=%d\n",m*n);
     cout<<"space for storing vector clock used by each thread= "<<n*sizeof(int)<<" bytes"<<endl;
     cout<<"space for sending the message used by each thread= "<<space/n<<" bytes"<<endl;

    return 0;

 }
    
  
