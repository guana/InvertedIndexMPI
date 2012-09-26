/* Copyright 2011
 Written by Josh Davidson <joshuad AT ualberta DOT ca>
 Based on an implementation written by Afsaneh Esteki and Anahita Alipour
 */
#include "MPI-InvertedIndex.h"

//----------------------Main----------------------------
int main(int argc, char *argv[])
{
    double start_time, total_time;
    
    // MPI_Init(0, 0);
    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    //The Mappers
    if(rank != 0){
        vector<string> files = vector<string>();
        string dir = string("./files/");
        
        getdir(dir,files);
        parseAndSend(&files);
        //fprintf(stderr,"Rank %d finished send\n",rank);
        //MPI_Recv(&flag,1,MPI_INT,0,TAG_INIT1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        //fprintf(stderr,"Rank %d received flag %d\n",rank,flag);
    }
    //The Reduce Step
    else{
        start_time = MPI_Wtime(); // start of time evaluation
        int i, j = 0;
        int total_num_files = 0;
        int files_per_node[size-1];
        for (i=1; i<size; i++){
            MPI_Recv(&files_per_node[i-1],1,MPI_INT,i,TAG_INIT1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            total_num_files += files_per_node[i-1];
        }
        while(j<total_num_files) {
            i=0;
            for (i=1; i<size; i++){
                if(files_per_node[i-1] > 0) {
                    gatherAndWrite(i, files_per_node[i-1]);
                    files_per_node[i-1]--;
                    j++;
                } 
            }
        }
        total_time = MPI_Wtime()-start_time;
        fprintf(stderr,"Total Runtime %d = %lf\n",rank,total_time);
    }
    
    MPI_Finalize();
    fprintf(stderr,"Process %d exiting\n",rank);
    
	return 0;
}

/* Parse the files specified by the files vector then send them to the Reducer*/
void parseAndSend(vector<string> *files) {
    string pagename, word;
    CombinerMap combinerMap;
    WordCountMap wordCountMap;
    
    double start_time, total_time=0;
    int i = 0, num_files = (int)files->size();

    /* Loop over the directory and process each file */
    MPI_Send(&num_files,1,MPI_INT,0,TAG_INIT1,MPI_COMM_WORLD);
    
    while(i < num_files)
    {
        start_time = MPI_Wtime(); // start of time evaluation
        string filename="./files/";
        filename.append(files->at(i));
        ifstream in(&filename[0] , ifstream::in );
        
        if(!in) {
            fprintf(stderr,"ERROR:Cannot open input file:%s",filename.c_str());
        }
        else
        {
            //Parse a single file into the map
            while (!in.eof())
            {
                in>>word;
                
                if(word.substr(0,3).compare("###")==0){
                    pagename = word.substr(3);
                    in>>word;
                    wordCountMap.insert(pair<string, int>(word, 1));
                    combinerMap.insert(pair<string, map<string,int> >(pagename, wordCountMap));
                    wordCountMap.clear();
                }
                else{
                    WordCountMap::iterator combinerMapIter = combinerMap[pagename].begin();
                    combinerMapIter = combinerMap[pagename].find(word);
                    if (combinerMapIter == combinerMap[pagename].end()) {
                        combinerMap[pagename].insert(pair<string, int >(word, 1));
                    }
                    else {
                        combinerMap[pagename][word]++;
                    }
                }
            }
            in.close();
            total_time += MPI_Wtime()-start_time;
            fprintf(stderr,"Map Time for Rank %d = %lf\n",rank,total_time);
            sendMap(&combinerMap);
            combinerMap.clear();
        }
        i++;
    }
    //total_time = MPI_Wtime()-start_time;
    //fprintf(stderr,"Map Time for Rank %d = %lf\n",rank,total_time);
    // Send the entries in the combinerMap to the reducer
    //sendMap(&combinerMap);
    //CombinerMap::iterator combinerMapIter((combinerMap.begin()));
    //combinerMap.erase(combinerMapIter,combinerMap.end());
    //combinerMap.clear();
    //combinerMap.clear();
}

/* Send the combiner map values to the master node */
void sendMap(CombinerMap *combinerMap) {
    
    string currentPage, key, value, package;
    char *c_package;
    int key_size, value_size;
    int package_size;
    int mapSize = (int)combinerMap->size();

    fprintf(stderr,"Ready to send <node %d>\n",rank);
    MPI_Ssend(&mapSize, 1 , MPI_INT, 0, TAG_INIT1,MPI_COMM_WORLD);
    for (CombinerMap::iterator combinerMapIter (combinerMap->begin()); combinerMapIter != combinerMap->end(); ++combinerMapIter){
        currentPage = combinerMapIter->first;
        package_size = 0;
        package = "";
        for (WordCountMap::iterator wordCountMapIter ((combinerMapIter->second).begin()); wordCountMapIter != (combinerMapIter->second).end(); ++wordCountMapIter){
            key = wordCountMapIter->first;
            value = currentPage+"="+ int2stringTT(wordCountMapIter->second);
            value.append(",");
            key_size = key.size();
            value_size = value.size();
            
            package.append(key);
            package.append(":");
            package.append(value);
            package.append(":");
            
        }
        package_size = (int)package.size()+1;
        //fprintf(stderr,"Package = %s\n",package.c_str());
        //Send the package to save on MPI calls
        //fprintf(stderr,"Package = %s\n",package.c_str());
        if(!(c_package = (char*)malloc((package_size)*sizeof(char)))){
            fprintf(stderr,"PACKAGE MALLOC FAILED ON NODE %d\n",rank);
            exit(-1);
        }
        strncpy(c_package,package.c_str(), package_size);
        //fprintf(stderr,"Sending package %d, size %d\n",++count, package_size);
        MPI_Send(&package_size, 1 , MPI_INT, 0, TAG_INIT2,MPI_COMM_WORLD);
        MPI_Send(c_package, package_size , MPI_CHAR, 0, TAG_INIT3,MPI_COMM_WORLD);
        free(c_package);
        
        //WordCountMap::iterator wordCountMapIter((combinerMapIter->second).begin());
        //(combinerMapIter->second).erase(wordCountMapIter,(combinerMapIter->second).end());
        //(combinerMapIter->second).clear();
    }
    //CombinerMap::iterator combinerMapIter((combinerMap->begin()));
    //combinerMap->erase(combinerMapIter,combinerMap->end());
    //combinerMap->clear();
}

/* Gather the messages from a node and store them in a map to create the inverted index. 
   Write the partial index afterwards*/
void gatherAndWrite(int sender, int file_num) {
    char *key, *value, *package;
    string str_key, str_value, str_package;
    int i,num_pages, package_size;
    InvertedIndex inv_index;
    
    MPI_Recv(&num_pages, 1 ,MPI_INT, sender, TAG_INIT1, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    fprintf(stderr,"Master node recieving %d pages from node %d\n",num_pages,sender);
    
    for(i=0;i<num_pages;i++){
        MPI_Recv(&package_size,1,MPI_INT,sender,TAG_INIT2, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        //fprintf(stderr,"Receiving package %d size %d\n",i,package_size);
        if(!(package = (char*)malloc(package_size*sizeof(char)))) {
            fprintf(stderr,"PACKAGE MALLOC FAILED ON NODE %d\n",rank);
            exit(-1);
        }
        MPI_Recv(package, package_size , MPI_CHAR, sender, TAG_INIT3,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        //fprintf(stderr,"Received package %d\n",i);
        
        key = strtok (package,":");
        value = key;
        while (key != NULL)
        {
            value = strtok(NULL,":");
            if(value == NULL) {
                break;
            }
            //fprintf(stderr,"Key = %s\t Value = %s\n",key, value);
            
            str_key.assign(key);
            str_value.assign(value);
            
            //fprintf(stderr,"Key = %s\t Value = %s\n",str_key.c_str(), str_value.c_str());
            
            InvertedIndex::iterator iter = inv_index.begin();
            iter = inv_index.find(str_key);
            
            if (iter == inv_index.end()){
                inv_index.insert(pair<string, string>(str_key, str_value));
            } 
            else{
                inv_index[str_key].append(str_value);
            }
            
            key = strtok (NULL, ":");
        }
        free(package);
    }
    writing(&inv_index,sender, file_num);
    inv_index.clear();
}

//----------------Write to file----------------------
int writing (InvertedIndex *counts, int sender, int file_num)
{
    char filename[255];
    sprintf(filename,"MPIPartialIndex-%d-%d",sender,file_num);
    fstream filestr;
    filestr.open (filename, fstream::in | fstream::out | fstream::app);
    
    fprintf(stderr,"Writing to file %s\n",filename);
    
    /* Writing to output*/
    for (InvertedIndex::iterator iter(counts->begin()); iter != counts->end();++iter) {
        filestr << iter->first << "\t" << iter->second << "\n";
    }
    
    filestr.close();
    
    return 0;
}

/* Put the filenames of the files directory into a vector of strings */
int getdir (string dir, vector<string> &files)
{
    DIR *dp;
    struct dirent *dirp;
    if((dp  = opendir(dir.c_str())) == NULL) {
        cout << "Error(" << errno << ") opening " << dir << endl;
        return errno;
    }
    
    while ((dirp = readdir(dp)) != NULL) {
        if(string(dirp->d_name)!="." && string(dirp->d_name)!="..")
        {
            files.push_back(string(dirp->d_name));
        }
    }
    closedir(dp);
    return 0;
}

/* Some Helpers */
string int2stringTT(int n){
	string s="";
	int k;
	while (n){
		k = n % 10;
		n = n / 10;
		s = (char)(k+'0') + s;
	}
	return s;
}

template <typename M> void FreeClear( M & amap ) {
    for ( typename M::iterator it = amap.begin(); it != amap.end(); ++it ) {
        delete &(it->second);
        delete &(it->first);
    }
    amap.clear();
}
