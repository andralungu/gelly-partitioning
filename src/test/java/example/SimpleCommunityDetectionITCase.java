package example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

@RunWith(Parameterized.class)
public class SimpleCommunityDetectionITCase extends MultipleProgramsTestBase {

	private String edgesPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public SimpleCommunityDetectionITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}
	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testSingleIteration() throws Exception {
		/*
		 * Test one iteration of the Simple Community Detection Example
		 */
		final String edges = "1	2\n" + "1	3\n" + "1	4\n" + "1	5\n" + "2	6\n" +
				"6	7\n" + "6	8\n" + "7	8";
		edgesPath = createTempFile(edges);

		SimpleCommunityDetectionExample.main(new String[] {edgesPath, resultPath, "1"});

		expected = "1,2\n" + "2,1\n" + "3,1\n" + "4,1\n" + "5,1\n" + "6,2\n" + "7,6\n" + "8,6";
	}

	@Test
	public void testTieBreaker() throws Exception {
		/*
		 * Test one iteration of the Simple Community Detection Example where a tie must be broken
		 */

		final String edges = "1	2\n" + "1	3\n" + "1	4\n" + "1	5";
		edgesPath = createTempFile(edges);

		SimpleCommunityDetectionExample.main(new String[] {edgesPath, resultPath, "1"});

		expected = "1,2\n" + "2,1\n" + "3,1\n" + "4,1\n" + "5,1";
	}


	// -------------------------------------------------------------------------
	// Util methods
	// -------------------------------------------------------------------------
	private String createTempFile(final String rows) throws Exception {
		File tempFile = tempFolder.newFile();
		Files.write(rows, tempFile, Charsets.UTF_8);
		return tempFile.toURI().toString();
	}
}
