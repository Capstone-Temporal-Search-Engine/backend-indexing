import unicodedata
import mmh3
import os

class CustomHashTable:
    def __init__(self, dict_size):
        self.size = dict_size * 3  # 3x dictionary size
        self.table = [None] * self.size  # Preallocate array

    def _hash(self, key):
        return mmh3.hash(key) % self.size  # Hash term to index

    def insert(self, key, value):
        index = self._hash(key)
        while self.table[index] is not None:  # Handle collisions (linear probing)
            index = (index + 1) % self.size
        self.table[index] = (key, value)

    def lookup(self, key):
        index = self._hash(key)
        while self.table[index] is not None:
            if self.table[index][0] == key:
                return self.table[index][1]
            index = (index + 1) % self.size
        return None  # Key not found

    def remove_accents(self, text):
        """Convert special characters (ć → c, č → c, é → e) to normal ASCII."""
        return ''.join(
            c for c in unicodedata.normalize('NFKD', text) if unicodedata.category(c) != 'Mn'
        )

    def write_to_dict_file(self, file_path):
        num_posting_counter = 0
        with open(file_path, 'w', encoding='utf-8') as f:
            for element in self.table:
                if element is not None:
                    normalized_first_item = self.remove_accents(element[0][:45])  # Normalize text
                    first_item = normalized_first_item.ljust(46)  # Ensure fixed width
                    second_item = str(len(element[1])).rjust(8)  # Right-justify
                    third_item = str(num_posting_counter).rjust(8)  # Right-justify
                    num_posting_counter += len(element[1])
                else:
                    first_item = "-1".ljust(46)
                    second_item = "-1".rjust(8)
                    third_item = "-1".rjust(8)

                record = f"{first_item} {second_item} {third_item}\n"
                f.write(record)

    def write_to_post_file(self, file_path):
        with open(file_path, 'w', encoding='utf-8') as f:
            for element in self.table:
                if element is None:
                    continue
                postings = element[1]
                for posting in postings:
                    num_record, scaled_tf = posting
                    record = str(num_record).ljust(8) + ' ' + str(scaled_tf).ljust(10)
                    f.write(record + '\n')