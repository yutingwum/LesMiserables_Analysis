import nltk

# I manually deleted the preamble and postabmble in the book, as permitted from comments on Piazza

def sentence_processing(input_filename, output_filename):
    # in a cleaned text file
    # use sentence dectector to make each sentence per line

    with open(input_filename, 'r') as input_file:
        sentences = input_file.read()
        with open(output_filename, 'w') as f:
            sentence_detector = nltk.data.load('tokenizers/punkt/english.pickle')
            f.write('\n'.join(sentence_detector.tokenize(sentences.strip().replace("\n", " "))))

        f.close()

    input_file.close()


def main():
    #process_text('lesmis.txt', 'lesmissentences.txt')
    sentence_processing('lesmis.txt', 'lesmissentences.txt')


if __name__ == '__main__':
    main()
