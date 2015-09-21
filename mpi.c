#include <mpi.h>
#include <omp.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAXUNIQUEWORD 100000
#define MAXWORDLENGTH 50

typedef struct wordTuple {
	char* key;
	int data;
	struct wordTuple* next;
} wordTuple;

void freeWordTuple(struct wordTuple* t){
	free(t->key);
	free(t);
}

void printList(struct wordTuple* list){
	printf("\n");
	while(list!=NULL){
		printf("%s:%d->", list->key, list->data);
		list=list->next;
	}
	printf("\n");
}

/* remove the punctuation and make the word to lowercase */
void remove_punct_and_make_lower_case(char *p)
{
	char *src = p, *dst = p;

	while (*src)
	{
       if (ispunct((unsigned char)*src)||isdigit((unsigned char)*src))
       {
          /* Skip this character */
          src++;
       }
       else if (isupper((unsigned char)*src))
       {
          /* Make it lowercase */
          *dst++ = tolower((unsigned char)*src);
          src++;
       }
       else if (src == dst)
       {
          /* Increment both pointers without copying */
          src++;
          dst++;
       }
       else
       {
          /* Copy character */
          *dst++ = *src++;
       }
    }

    *dst = 0;
}

/* read word from a file  */
struct wordTuple* ReadWords(const char *filename)
{
    FILE *f = fopen(filename, "rt"); // checking for NULL is boring; i omit it
    char temp[100]; // assuming the words cannot be too long 
    struct wordTuple *first, *cur, *pre=NULL;
    pre = NULL;
    while (1) //scan the file until hit the end-of-file
    {
        // Read a word from the file
        if (fscanf(f, "%s", temp) != 1)
            break;
        // note: "!=1" checks for end-of-file; using feof for that is usually a bug
        remove_punct_and_make_lower_case(temp);
        cur = (struct wordTuple *) malloc(sizeof(wordTuple));
        cur->key = strdup(temp);
        cur->next = NULL;
        cur->data = 1;
        if(pre!=NULL){pre->next = cur;}else{first=cur;}
        pre = cur;
    }
    fclose(f);
    return first;
}

void reader(const char *filename, struct wordTuple** workQueue_ptr){
	*workQueue_ptr = ReadWords(filename);
}

struct wordTuple* map(struct wordTuple* wordlist){
	struct wordTuple *result=NULL, *cur, *pre=NULL, *search;
	struct wordTuple *temp;
	while(wordlist!=NULL){
			search=NULL;
			temp=result;
			while(temp!=NULL){
				if(!strcmp(wordlist->key,temp->key)){
					search=temp;
					break;
				}
        temp=temp->next;
			}
			if(search!=NULL){
				search->data+=wordlist->data;
			}else{
				cur = (struct wordTuple *) malloc(sizeof(wordTuple));
				cur->key=strdup(wordlist->key);
				cur->data=wordlist->data;
				cur->next=NULL;
        if(pre!=NULL){pre->next=cur;}else{result=cur;}
				pre=cur;
			}
    cur = wordlist;
    wordlist=wordlist->next;
    freeWordTuple(cur);
	}
	return result;
}

void mapper(struct wordTuple** mapresult,struct wordTuple* wordlist ){
	*mapresult=map(wordlist);
}

/* hash function of reducer putting word to reduce queue */
void hash(int* reduceWorkQueue[], char* reduceWord[], char* word, int num, int* reduceCount_ptr, int idx, int node_num){
	int i;	
	for (i=0;i<*reduceCount_ptr;i++){
		if(!strcmp(word,reduceWord[i])){
			reduceWorkQueue[i][idx] = num;
			return;
		}
	}

  	//if this is a new word, create one in the matrix
	reduceWord[*reduceCount_ptr]=strdup(word);
	int* numArray = (int *) malloc(sizeof(int)*node_num); 
	for(i=0; i<node_num; i++){
		numArray[i]=0;
	}
	numArray[idx] = num;
	reduceWorkQueue[*reduceCount_ptr] = numArray;
	(*reduceCount_ptr)++;
}

/* local hash function of gathering putting mapper result*/
void local_hash(int local_reduce[], char* local_reduce_word[], char* word, int num, int* reduceCount_ptr){
	int i;	
	for (i=0;i<*reduceCount_ptr;i++){
		if(!strcmp(word,local_reduce_word[i])){
			local_reduce[i] = local_reduce[i]+num;
			return;
		}
	}

	local_reduce_word[*reduceCount_ptr]=strdup(word);
	local_reduce[*reduceCount_ptr] = num;
	(*reduceCount_ptr)++;
}

void reducer(int reduceWorkQueuePerWord[], char *word, struct wordTuple** reduceResult_ptr, int node_num){
	int i, sum=0;
	for(i=0; i<node_num; i++){
		sum+=reduceWorkQueuePerWord[i];
	}
	(*reduceResult_ptr) = (struct wordTuple *) malloc(sizeof(wordTuple));
	(*reduceResult_ptr)->key = strdup(word);
	(*reduceResult_ptr)->data = sum;
	(*reduceResult_ptr)->next = NULL;
}

void writer(struct wordTuple* reduceResult[], int wordCount){
	FILE *f = fopen("result.txt", "w");
	if (f == NULL)
	{
		printf("Error opening file!\n");
		exit(1);
	}
	fprintf(f, "This is the result of mapReduce:\n");
	int i;
	for(i = 0; i<wordCount; ++i){
		fprintf(f, "Word: %s, Count: %d\n", reduceResult[i]->key, reduceResult[i]->data);
		freeWordTuple(reduceResult[i]);
	}

	fclose(f);
}

/* hash function of decideing reducer word load */
void reduce_hash(int node, int letter_index[]){
	int total = 26, i=0;
	int ave = total/node;
	int left = total%node;
	while(left>0){
		if(i==0){letter_index[i] = ave;} //index from 0
		else{letter_index[i] = letter_index[i-1]+ave+1;}
		i=i+1;
		left=left-1;
	}
	while(i<node){
		if(i==0){letter_index[i] = ave-1;} //index from 0
		else{letter_index[i] = letter_index[i-1]+ave;}
		i=i+1;
	}
}

void getWord(char* word, int* count, char wordarr[]){
	int i=0;
	while((*word)!='\0'){
		wordarr[i] = *word;
		word++; i++;
	}
	wordarr[i]='\0';
	*count = i+1;
}

/* get reducer node rank */
int getDest(char* string, int letter_index[]){
  int i=0;
  int diff=string[0]-'a';
  if(diff<letter_index[0] || diff>25)
    return 0;
  while(letter_index[i]!=25){
    i++;
    if(diff<letter_index[i]&& diff>letter_index[i-1])
      return i;
 } return 0;
}

int main(int argc, char *argv[]){

	double duration;
	int i,  node_file_num=16; 

	char* fileNames[32] = {"1.txt", "2.txt", "3.txt","4.txt","5.txt","6.txt","7.txt","8.txt","9.txt","10.txt","11.txt",
	"12.txt","13.txt","14.txt","15.txt","16.txt","17.txt","18.txt","19.txt",
	"20.txt","21.txt", "22.txt","23.txt","24.txt","25.txt","26.txt","27.txt","28.txt","29.txt",
	"30.txt","31.txt", "32.txt"}; //init file names

	char* node_filenames[16]; //files for each node to process

  	struct wordTuple* mapWorkQueue[node_file_num];
	struct wordTuple* mapResult[MAXUNIQUEWORD];
	struct wordTuple *reduceResult[MAXUNIQUEWORD];
	struct wordTuple *finalResult[MAXUNIQUEWORD];
	int finalresultCount = 0;

	int reduceWorkCount=0;
	char* reduceWorkWord[MAXUNIQUEWORD];
	int* reduceWorkQueue[MAXUNIQUEWORD];

	int local_reduce_count=0;
	char* local_reduce_word[MAXUNIQUEWORD];
	int local_reduce_queue[MAXUNIQUEWORD];

	char word_buffer[MAXWORDLENGTH];
 
 	int world_rank, world_size;
  	MPI_Init(&argc, &argv); 
  	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  	MPI_Status status;
  	MPI_Request request;

  	int* reduce_distribute_index = (int *) malloc(sizeof(int)*world_size);

  	if(world_rank == 0){
  		duration = -MPI_Wtime();
  		for(i=0;i<node_file_num;i++){
             node_filenames[i]=fileNames[i+world_rank*node_file_num];
        }

  		omp_set_num_threads(16);
		#pragma omp parallel for 
		for(i=0; i<node_file_num; ++i){
			reader(node_filenames[i], &mapWorkQueue[i]);
		}
	  
		#pragma omp parallel for
		for(i=0;i<node_file_num;i++){
			mapper(&mapResult[i],mapWorkQueue[i]);
	    	struct wordTuple *temp, *cur = mapResult[i];
		    while(cur!=NULL){
				#pragma omp critical
				local_hash(local_reduce_queue, local_reduce_word, cur->key, cur->data, &local_reduce_count);
				temp=cur; 
				cur=cur->next; 
				freeWordTuple(temp);
			}
		}

		MPI_Barrier(MPI_COMM_WORLD);

		//get hash index
		reduce_hash(world_size, reduce_distribute_index);

		/*---------distribute reduce work to reduce node based on a hash function---------*/
		#pragma omp parallel for
		for(i=0;i<local_reduce_count;i++){//send word
		  char word_buffer[MAXWORDLENGTH];
		  int length;
		  getWord(local_reduce_word[i],&length,word_buffer); // store word from char* to local array
		  int dest=getDest(local_reduce_word[i],reduce_distribute_index);
		  if(dest==world_rank){
		  	hash(reduceWorkQueue, reduceWorkWord, local_reduce_word[i],local_reduce_queue[i],&reduceWorkCount, world_rank, world_size);
		  }else{
		  	MPI_Isend(word_buffer,length,MPI_CHAR,dest,0,MPI_COMM_WORLD, &request);
		  	MPI_Isend(&local_reduce_queue[i],1,MPI_INT,dest,1,MPI_COMM_WORLD, &request);
		  }
		}
		for(i=0;i<world_size;i++){//send end char
		  char a ='@';
		  int b=0;
		  if(i==world_rank) continue;
		  MPI_Isend(&a,1,MPI_CHAR,i,0,MPI_COMM_WORLD, &request);
		  MPI_Isend(&b,1,MPI_INT,i,1,MPI_COMM_WORLD, &request);
		}
		int flag=0;//use this parameter to count how many nodes already finish sending
		while(flag!=world_size-1){
		  int count, num, source; 
		  MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		  MPI_Get_count(&status, MPI_CHAR, &count); source = status.MPI_SOURCE;
		  char buffer[count];
		  MPI_Recv(buffer,count,MPI_CHAR,MPI_ANY_SOURCE,0,MPI_COMM_WORLD, &status);
		  MPI_Recv(&num,1,MPI_INT,MPI_ANY_SOURCE,1,MPI_COMM_WORLD, &status);
		  if(buffer[0]=='@'){
		  	flag++;
		  }else{
		  	hash(reduceWorkQueue, reduceWorkWord, buffer,num,&reduceWorkCount, source, world_size);
		  }
		}
		free(reduce_distribute_index);

		/*---------reduce based on reduce queue get from other nodes---------*/
		#pragma omp parallel for
		for (i = 0; i<reduceWorkCount; ++i){
			reducer(reduceWorkQueue[i], reduceWorkWord[i], &finalResult[i], world_size);
			free(reduceWorkQueue[i]);
		}

		/*---------gather reduce work from other nodes--------*/
		flag=0;
		finalresultCount = reduceWorkCount; // already stored data from node 0
		while(flag!=world_size-1){
		  int count;
		  int num;
		  MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		  MPI_Get_count(&status, MPI_CHAR, &count);
		  char buffer[count];
		  MPI_Recv(buffer,count,MPI_CHAR,MPI_ANY_SOURCE,0,MPI_COMM_WORLD, &status);
		  MPI_Recv(&num,1,MPI_INT,MPI_ANY_SOURCE,1,MPI_COMM_WORLD, &status);
		  if(buffer[0]=='@')flag++;
		  else{
		  	finalResult[finalresultCount] = (struct wordTuple *) malloc(sizeof(wordTuple));
		  	finalResult[finalresultCount]->key = strdup(buffer);
		  	finalResult[finalresultCount]->data = num;
	      	finalResult[finalresultCount]->next = NULL;
	      	finalresultCount++;
	  	   }
		 }

  		writer(finalResult, finalresultCount);
  		duration += MPI_Wtime();
  		printf( "Total time process %d files on %d nodes is %f seconds\n", world_size*node_file_num, world_size, duration); 

  	}else{
  		for(i=0;i<node_file_num;i++){
             node_filenames[i]=fileNames[i+world_rank*node_file_num];
        }

		omp_set_num_threads(16);
		#pragma omp parallel for 
		for(i=0; i<node_file_num; ++i){
			reader(node_filenames[i], &mapWorkQueue[i]);
		}
	  
		#pragma omp parallel for
		for(i=0;i<node_file_num;i++){
			mapper(&mapResult[i],mapWorkQueue[i]);
	    	struct wordTuple *temp, *cur = mapResult[i];
		    while(cur!=NULL){
				#pragma omp critical
				local_hash(local_reduce_queue, local_reduce_word, cur->key, cur->data, &local_reduce_count);
				temp=cur; 
				cur=cur->next; 
				freeWordTuple(temp);
			}
		}

		MPI_Barrier(MPI_COMM_WORLD);

		//get hash index
		reduce_hash(world_size, reduce_distribute_index);

		/*---------distribute reduce work to reduce node based on a hash function---------*/
		#pragma omp parallel for
		for(i=0;i<local_reduce_count;i++){//send word
		  char word_buffer[MAXWORDLENGTH];
		  int length;
		  getWord(local_reduce_word[i],&length,word_buffer);
		  int dest=getDest(local_reduce_word[i],reduce_distribute_index);
		  if(dest==world_rank){
		  	hash(reduceWorkQueue, reduceWorkWord, local_reduce_word[i],local_reduce_queue[i],&reduceWorkCount, world_rank, world_size);
		  }else{
		  	MPI_Isend(word_buffer,length,MPI_CHAR,dest,0,MPI_COMM_WORLD, &request);
		  	MPI_Isend(&local_reduce_queue[i],1,MPI_INT,dest,1,MPI_COMM_WORLD, &request);
		  }
		}
		for(i=0;i<world_size;i++){//send end char
		  char a ='@';
		  int b=0;
		  if(i==world_rank) continue;
		  MPI_Isend(&a,1,MPI_CHAR,i,0,MPI_COMM_WORLD, &request);
		  MPI_Isend(&b,1,MPI_INT,i,1,MPI_COMM_WORLD, &request);
		}
		int flag=0;//use this parameter to count how many nodes already finish sending
		while(flag!=world_size-1){
		  int count, num, source; 
		  MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		  MPI_Get_count(&status, MPI_CHAR, &count); source = status.MPI_SOURCE;
		  char buffer[count];
		  MPI_Recv(buffer,count,MPI_CHAR,MPI_ANY_SOURCE,0,MPI_COMM_WORLD, &status);
		  MPI_Recv(&num,1,MPI_INT,MPI_ANY_SOURCE,1,MPI_COMM_WORLD, &status);
		  if(buffer[0]=='@'){
		  	flag++;
		  }else{
		  	hash(reduceWorkQueue, reduceWorkWord, buffer,num,&reduceWorkCount, source, world_size);
		  }
		}

		/*---------reduce based on reduce queue get from other nodes---------*/
		#pragma omp parallel for
		for (i = 0; i<reduceWorkCount; ++i){
			reducer(reduceWorkQueue[i], reduceWorkWord[i], &reduceResult[i], world_size); 
			int length;
			getWord(reduceResult[i]->key,&length,word_buffer);
			/* send reduce result to main node 0*/
			MPI_Send(word_buffer,length,MPI_CHAR,0,0,MPI_COMM_WORLD);
		  	MPI_Send(&(reduceResult[i]->data),1,MPI_INT,0,1,MPI_COMM_WORLD);
			free(reduceWorkQueue[i]);
		}

		//end
		char a ='@'; int b=0;
		MPI_Send(&a,1,MPI_CHAR,0,0,MPI_COMM_WORLD);
		MPI_Send(&b,1,MPI_INT,0,1,MPI_COMM_WORLD);

		//free reduce result
		for(i = 0; i<reduceWorkCount; ++i){
			freeWordTuple(reduceResult[i]);		
		}

  	}

  	MPI_Finalize(); 
	
	return EXIT_SUCCESS;
}
