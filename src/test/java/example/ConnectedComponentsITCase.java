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
import util.ConnectedComponentsData;

import java.io.File;


@RunWith(Parameterized.class)
public class ConnectedComponentsITCase extends MultipleProgramsTestBase {

	private String edgesPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public ConnectedComponentsITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();

		File edgesFile = tempFolder.newFile();
		Files.write(ConnectedComponentsData.EDGES, edgesFile, Charsets.UTF_8);
		edgesPath = edgesFile.toURI().toString();
	}

	@Test
	public void testConnectedComponentsExample() throws Exception {
		ConnectedComponents.main(new String[]{edgesPath, resultPath, ConnectedComponentsData.MAX_ITERATIONS + ""});
		expected = ConnectedComponentsData.VERTICES_WITH_MIN_ID;
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}
}
