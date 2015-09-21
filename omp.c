#include <omp.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAXUNIQUEWORD 100000

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

// remove the punctuation and make the word to lowercase
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

// read word from a file 
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

void hash(int* reduceWorkQueue[], char* reduceWord[], char* word, int num, int* reduceCount_ptr, int idx, int file_num){
	int i;	
	for (i=0;i<*reduceCount_ptr;i++){
		if(!strcmp(word,reduceWord[i])){
			reduceWorkQueue[i][idx] = num;
			return;
		}
	}
  //if this is a new word, create one in the matrix
	reduceWord[*reduceCount_ptr]=strdup(word);
	int* numArray = (int *) malloc(sizeof(int)*file_num); 
	for(i=0; i<file_num; i++){
		numArray[i]=0;
	}
	numArray[idx] = num;
	reduceWorkQueue[*reduceCount_ptr] = numArray;
	(*reduceCount_ptr)++;
}

void reducer(int reduceWorkQueuePerWord[], char *word, struct wordTuple** reduceResult_ptr, int file_num){
	int i, sum=0;
	for(i=0; i<file_num; i++){
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

int main(int argc, char const *argv[]){

	double time;
	time=-omp_get_wtime();
	
	if (argc==1){
		printf("Error! There is no input file!\n");
		return 1;
	}
  
	int i;
	int file_num=argc-1; //the number of files
  
	struct wordTuple* mapWorkQueue[file_num];

	#pragma omp parallel for 
	for(i=0; i<file_num; ++i){
		reader(argv[i+1], &mapWorkQueue[i]);
	}

	struct wordTuple* mapResult[MAXUNIQUEWORD];

	int reduceCount=0;
	char* reduceWord[MAXUNIQUEWORD];
	int* reduceWorkQueue[MAXUNIQUEWORD];
  
	#pragma omp parallel for
	for(i=0;i<file_num;i++){
		mapper(&mapResult[i],mapWorkQueue[i]);
    struct wordTuple *temp, *cur = mapResult[i];
    while(cur!=NULL){
		#pragma omp critical
		hash(reduceWorkQueue, reduceWord, cur->key, cur->data, &reduceCount, i, file_num);
		temp=cur; 
		cur=cur->next; 
		freeWordTuple(temp);
		}
	}

	struct wordTuple *reduceResult[MAXUNIQUEWORD];

	#pragma omp parallel for
	for (i = 0; i<reduceCount; ++i){
		reducer(reduceWorkQueue[i], reduceWord[i], &reduceResult[i], file_num);
		free(reduceWorkQueue[i]);
	}

	writer(reduceResult, reduceCount);
	
	time+=omp_get_wtime();
	printf("Time = %f\n",time);
	return EXIT_SUCCESS;
}
