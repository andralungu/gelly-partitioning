package example;

import example.util.JaccardSimilarityMeasureData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

@RunWith(Parameterized.class)
public class JaccardSimilarityMeasureITCase extends MultipleProgramsTestBase {

	private String edgesPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public JaccardSimilarityMeasureITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();

		File edgesFile = tempFolder.newFile();
		Files.write(JaccardSimilarityMeasureData.EDGES, edgesFile, Charsets.UTF_8);

		edgesPath = edgesFile.toURI().toString();
	}

	@Test
	public void testJaccardSimilarityMeasureExample() throws Exception {
		JaccardSimilarityMeasureExample.main(new String[]{edgesPath, resultPath,
				JaccardSimilarityMeasureData.MAX_ITERATIONS + ""});
		expected = JaccardSimilarityMeasureData.JACCARD_VERTICES;
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}
}
