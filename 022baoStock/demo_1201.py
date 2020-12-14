from functools import reduce
import numpy as np

texts = [['i', 'have', 'a', 'cat'],
         ['he', 'have', 'a', 'dog'],
         ['he', 'and', 'i', 'have', 'a', 'cat', 'and', 'a', 'dog']]
dictionary = list(enumerate(set(list(reduce(lambda x, y: x + y, texts)))))
def test(text):

    vector = np.zeros(len(dictionary))
    print(vector)
    for i, word in dictionary:
        num = 0
        for w in text:
            if w == word:
                print(w)

if __name__ == '__main__':
    for t in texts:
        test(t)
