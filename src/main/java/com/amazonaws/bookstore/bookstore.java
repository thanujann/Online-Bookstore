package com.amazonaws.bookstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.sun.istack.internal.NotNull;

public class bookstore {

	final static String bookstoreTableName = "Books";
	final static String hashKeyName = "Author";
	final static String sortKeyName = "Title";
	final static String categoryIndexHashKeyName = "Category";
	final static String categoryIndexName = "CategoryIndex";

	enum BookFormat {
		HARDCOVER, PAPERBACK, AUDIOBOOK
	}

	// Create a DynamoDB table
	public static Table createBookstore() throws Exception {
		try {
			// Get DynamoDB instance
			AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
			DynamoDB dynamoDB = new DynamoDB(client);

			// Table key schema (composite primary key on Author (HashKey) and Title
			// (SortKey))
			List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
			keySchema.add(new KeySchemaElement().withAttributeName(hashKeyName).withKeyType(KeyType.HASH));
			keySchema.add(new KeySchemaElement().withAttributeName(sortKeyName).withKeyType(KeyType.RANGE));

			// Table attribute definitions
			List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(hashKeyName).withAttributeType("S"));
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(sortKeyName).withAttributeType("S"));
			attributeDefinitions
					.add(new AttributeDefinition().withAttributeName(categoryIndexHashKeyName).withAttributeType("S"));

			// Global secondary index on category attribute
			GlobalSecondaryIndex categoryIndex = new GlobalSecondaryIndex().withIndexName(categoryIndexName)
					.withProvisionedThroughput(
							new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L))
					.withProjection(new Projection().withProjectionType(ProjectionType.ALL));

			// Global secondary index key schema
			ArrayList<KeySchemaElement> categoryIndexKeySchema = new ArrayList<KeySchemaElement>();
			categoryIndexKeySchema
					.add(new KeySchemaElement().withAttributeName(categoryIndexHashKeyName).withKeyType(KeyType.HASH));
			categoryIndex.setKeySchema(categoryIndexKeySchema);

			// Create table with key schema, attribute definitions, provisioned throughput,
			// and global secondary index
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(bookstoreTableName)
					.withKeySchema(keySchema).withAttributeDefinitions(attributeDefinitions)
					.withProvisionedThroughput(
							new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L))
					.withGlobalSecondaryIndexes(categoryIndex);

			Table bookstore = dynamoDB.createTable(createTableRequest);
			bookstore.waitForActive();

			return bookstore;
		} catch (Exception e) {
			System.err.println("Error creating table.");
			System.err.println(e.getMessage());
			throw e;
		}
	}

	public static void addBooks(@NotNull Table bookstore) {
		addBook(bookstore, "John Grisham", "The Rainmaker", "Suspense", new HashMap<String, String>() {
			{
				put(BookFormat.HARDCOVER.name(), "J4SUKVGU");
				put(BookFormat.PAPERBACK.name(), "D7YF4FCX");
			}
		});

		addBook(bookstore, "John Grisham", "The Firm", "Suspense", new HashMap<String, String>() {
			{
				put(BookFormat.HARDCOVER.name(), "Q7QWE3U2");
				put(BookFormat.PAPERBACK.name(), "ZVZAYY4F");
				put(BookFormat.AUDIOBOOK.name(), "DJ9KS9NM");
			}
		});

		addBook(bookstore, "James Patterson", "Along Came a Spider", "Suspense", new HashMap<String, String>() {
			{
				put(BookFormat.HARDCOVER.name(), "C9NR6RJ7");
				put(BookFormat.PAPERBACK.name(), "37JVGDZG");
				put(BookFormat.AUDIOBOOK.name(), "6348WX3U");
			}
		});

		addBook(bookstore, "Dr. Seuss", "Green Eggs and Ham", "Children", new HashMap<String, String>() {
			{
				put(BookFormat.HARDCOVER.name(), "GVJZQ7JK");
				put(BookFormat.PAPERBACK.name(), "A4TFUR98");
				put(BookFormat.AUDIOBOOK.name(), "XWMGHW96");
			}
		});

		addBook(bookstore, "William Shakespeare", "Hamlet", "Drama", new HashMap<String, String>() {
			{
				put(BookFormat.HARDCOVER.name(), "GVJZJ7JK");
				put(BookFormat.PAPERBACK.name(), "A4TFFR98");
				put(BookFormat.AUDIOBOOK.name(), "XWMGEW96");
			}
		});
	}

	// Insert an item into the table
	public static void addBook(@NotNull Table bookstore, @NotNull String author, @NotNull String title,
			@NotNull String category, @NotNull HashMap<String, String> formats) {
		final String formatsAttributeName = "Formats";
		Item item = new Item().withPrimaryKey(hashKeyName, author).withPrimaryKey(sortKeyName, title)
				.withString(categoryIndexHashKeyName, category).withMap(formatsAttributeName, formats);

		bookstore.putItem(item);
	}

	// Retrieve an item from the table
	public static Item getBook(@NotNull Table bookstore, @NotNull String author, @NotNull String title) {
		GetItemSpec itemSpec = new GetItemSpec().withPrimaryKey(hashKeyName, author, sortKeyName, title);

		return bookstore.getItem(itemSpec);
	}

	// Query to retrieve multiple items
	public static ItemCollection<QueryOutcome> getBooksByAuthor(@NotNull Table bookstore, @NotNull String author) {
		QuerySpec querySpec = new QuerySpec().withKeyConditionExpression(hashKeyName + " = :name")
				.withValueMap(new ValueMap().withString(":name", author));

		return bookstore.query(querySpec);
	}

	// Query by the global secondary index
	public static ItemCollection<QueryOutcome> getBooksByCategory(@NotNull Table bookstore, @NotNull String category) {
		Index categoryIndex = bookstore.getIndex(categoryIndexName);
		QuerySpec indexSpec = new QuerySpec().withKeyConditionExpression(categoryIndexHashKeyName + " = :name")
				.withValueMap(new ValueMap().withString(":name", category));

		return categoryIndex.query(indexSpec);
	}

	// Update an item in the table
	public static void addBookFormat(@NotNull Table bookstore, @NotNull String author, @NotNull String title,
			@NotNull BookFormat format, @NotNull String formatId) {
		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(hashKeyName, author, sortKeyName, title)
				.withUpdateExpression("set Formats." + format.name() + " = :id")
				.withValueMap(new ValueMap().withString(":id", formatId));

		bookstore.updateItem(updateItemSpec);
	}

	// Delete the DynamoDB table
	public static void deleteBookstore(@NotNull Table bookstore) {
		try {
			bookstore.delete();
			bookstore.waitForDelete();
		} catch (Exception e) {
			System.err.println("Error deleting table.");
			System.err.println(e.getMessage());
		}
	}

	public static void main(String[] args) throws Exception {
		// Create the bookstore
		Table bookstore = createBookstore();

		// Add books to the bookstore
		addBooks(bookstore);

		// Get a book titled The Rainmaker by John Grisham
		Item theRainmakerBook = getBook(bookstore, "John Grisham", "The Rainmaker");
		System.out.println("The Rainmaker by John Grisham:");
		System.out.println(theRainmakerBook.toJSON()); // Print information about the book

		// Get all books made by John Grisham
		ItemCollection<QueryOutcome> johnGrishamBooks = getBooksByAuthor(bookstore, "John Grisham");
		System.out.println("John Grisham Books:"); // Print information about each returned book
		for (Item book : johnGrishamBooks) {
			System.out.println(book.toJSON());
		}

		// Get all books in the Suspense category
		ItemCollection<QueryOutcome> suspenseBooks = getBooksByCategory(bookstore, "Suspense");
		System.out.println("Suspense Books:"); // Print information about each returned book
		for (Item book : suspenseBooks) {
			System.out.println(book.toJSON());
		}

		// Add a format to an existing book
		addBookFormat(bookstore, "John Grisham", "The Rainmaker", BookFormat.AUDIOBOOK, "8WE3KPTP");
		System.out.println("The Rainmaker by John Grisham (Updated):");
		theRainmakerBook = getBook(bookstore, "John Grisham", "The Rainmaker");
		System.out.println(theRainmakerBook.toJSON()); // Print updated book information

		// Delete the bookstore
		deleteBookstore(bookstore);
	}

}