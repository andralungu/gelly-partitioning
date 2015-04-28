package example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import example.util.TriangleCountData;
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
public class GSATriangleCountITCase extends MultipleProgramsTestBase {

	private String edgesPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public GSATriangleCountITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
		File edgesFile = tempFolder.newFile();
		Files.write(TriangleCountData.EDGES, edgesFile, Charsets.UTF_8);
		edgesPath = edgesFile.toURI().toString();
	}

	@Test
	public void testGSATriangleCount() throws Exception {
		GSATriangleCount.main(new String[]{edgesPath, resultPath, TriangleCountData.MAX_ITERATIONS + ""});
		expected = TriangleCountData.RESULTED_NUMBER_OF_TRIANGLES;
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}
}
