# Online Bookstore
This online bookstore runs on a DynamoDB database (non-relational), where data is stored and retrieved through the 
use of the AWS SDK for Java. It is an adaptation of Amazon's online bookstore application 
that was written in Python (using Boto 3). Although simpler in Python, this project was done
in Java to play around with the AWS SDK for Java, and to compare the differences between
the Python and Java implementations.

The usage of a DynamoDB database is important for this project because it is an online 
bookstore that can have a wide variety of books stored. Therefore, DynamoDB's low-latency 
performance and scaling-capabilities are highly desired.

## Data Model
1. Author (the author of the book) - String
2. Title (the title of the book) - String
3. Category (the category of the book) - String
4. Formats (the available formats of the book (hardcopy, paperback, audiobook) and their corresponding ids) - Map

## Implementation Procedure
1. The DynamoDB table (bookstore) was created, with the appropriate primary key (author + title) and attribute definitions.
2. A global secondary index (on the category attribute) was added to the table.
3. Items (books) were then loaded into the table.
4. It was made possible to retrieve multiple items based on the hash key (author) of the primary key.
5. It was made possible to query by the secondary index to retrieve all items with a specific attribute.
6. It was made possible to update any item in the table.
7. The table was deleted to free the AWS resources used.
