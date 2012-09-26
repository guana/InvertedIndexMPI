/* Copyright 2011
 Written by Josh Davidson <joshuad AT ualberta DOT ca>
 Based on an implementation written by Afsaneh Esteki and Anahita Alipour
 */
#ifndef _MPI_InvertedIndex_h
#define _MPI_InvertedIndex_h
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <cctype>
#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <string>
#include <cmath>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <vector>
#include <sstream>

using namespace std;
#define TAG_INIT1 31337
#define TAG_INIT2 25000
#define TAG_INIT3 26000
#define TAG_INIT4 27000

typedef std::map<string, string> StringMap;
typedef std::map<string, string> InvertedIndex;
typedef std::map<string, int> WordCountMap;
typedef std::map<string, map<string,int> > CombinerMap;

int rank, size;

int getdir (string dir, vector<string> &files);
int writing (InvertedIndex *counts, int sender,int file_num);
void gatherAndWrite(int sender, int file_num);
void parseAndSend(vector<string> *files);
void sendMap(CombinerMap *combinerMap);

string int2stringTT(int n);

#endif