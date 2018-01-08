import nltk
from nltk.tokenize import RegexpTokenizer
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRBigramCounter(MRJob):
    # use a step function to specify the steps
    # the job includes one mapper, one combiner, and two reducers
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_bigram,
                   combiner=self.combiner_count_bigrams),
            MRStep(reducer=self.reducer_count_bigrams),
            MRStep(reducer=self.sort_bigram_count)
        ]

    def mapper_get_bigram(self, _, line):
        # in the mapper
        # tokenizer to separate words in a given sentence
        tokenizer = RegexpTokenizer('\w+')
        words = tokenizer.tokenize(line)
        # add the list of stop words
        stop_words = ['their', 'she', 'did', 'not', 'needn', 'have', 'all', 'a', 'has', 'between', 'shouldn', 'where', 'these', 'had', 'ours', 'who', 'further', 'does', 's', 't', 'are', 'isn', 'should', 'both', 'against', 'll', 're', 'can', 'that', 'few', 'out', 'no', 'hers', 'myself', 'but', 'at', 'too', 'once', 'the', 'there', 'o', 'this', 'down', 'in', 'some', 'and', 'weren', 'we', 'own', 'into', 'don', 'other', 'him', 'during', 'himself', 'having', 'them', 'why', 'ain', 'each', 'it', 'when', 'were', 'will', 'mightn', 'very', 'aren', 'am', 'mustn', 'they', 'ourselves', 'only', 'd', 'or', 'than', 'if', 'itself', 'from', 'i', 'being', 'her', 'me', 'after', 'yourselves', 'more', 'yours', 'through', 'those', 'of', 'you', 'doesn', 'about', 'to', 'y', 'your', 'doing', 'just', 'herself', 'now', 'wouldn', 'its', 'been', 'under', 'hadn', 'wasn', 'above', 'any', 'nor', 'over', 'because', 'on', 'shan', 'themselves', 've', 'off', 'while', 'then', 'how', 'so', 'until', 'most', 'our', 'up', 'is', 'yourself', 'was', 'what', 'before', 'which', 'same', 'again', 'didn', 'haven', 'ma', 'be', 'do', 'with', 'won', 'm', 'couldn', 'whom', 'my', 'theirs', 'below', 'such', 'for', 'his', 'an', 'by', 'hasn', 'as', 'here', 'he']

        # create bigrams
        bigrams = nltk.bigrams(words)
        for bigram in bigrams:
            # for each bigram in the list of bigrams
            # convert them to lowercase first
            first = bigram[0].lower()
            second = bigram[1].lower()
            bigram = (first, second)

            # add a check variable to make sure the words in the bigram are no the stop words list
            check = True

            # for each work in a bigram
            for word in bigram:
                if word == 'th\\u00e9nardier':
                    print('------ HIT --------')
                    word = 'Thénardier'
                    print(word)
                # if the word is in the stop word list
                if word.lower() in stop_words:
                    check = False
            try:
                # if it is NOT in the stop words
                if check:
                    # yield the count of the bigram
                    yield bigram, 1
            except:
                pass

    def combiner_count_bigrams(self, key, values):
        # use a combiner to count occurrences of bigram
        # the combiner can reduce the workload of the reducer as it computes subsets of the key value pair
        yield key, sum(values)

    def reducer_count_bigrams(self, key, values):
        # use the reducer to count the occurrences of the bigrams
        # yield a (None, (sum(values), key)) tuple, for sorting purpose
        yield None, (sum(values), key)


    def sort_bigram_count(self, count, bigrams):
        # sort the bigram list by the occurrences
        sort_bigrams = sorted(bigrams, key=lambda x: x[0], reverse=True)
        # add a counter variable to print the top 50
        counter = 0
        while counter < 50:
            yield sort_bigrams[counter]
            counter = counter + 1

if __name__ == '__main__':
    MRBigramCounter.run()