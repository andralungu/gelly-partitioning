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
import util.NodeSplittingData;

import java.io.File;

@RunWith(Parameterized.class)
public class CountDegreeITCase extends MultipleProgramsTestBase {

	private String edgesPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public CountDegreeITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
		File edgesFile = tempFolder.newFile();
		Files.write(NodeSplittingData.EDGES, edgesFile, Charsets.UTF_8);
		edgesPath = edgesFile.toURI().toString();
	}

	@Test
	public void testCountDegree() throws Exception {
		AlphaNodeSplitting.main(new String[]{edgesPath, resultPath, NodeSplittingData.MAX_ITERATIONS + "",
				NodeSplittingData.ALPHA + ""});
		expected = NodeSplittingData.VERTICES_WITH_DEGREES;
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}
}
