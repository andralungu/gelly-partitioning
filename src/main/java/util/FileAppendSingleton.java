package util;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;

public class FileAppendSingleton {
	private static final FileAppendSingleton inst= new FileAppendSingleton();

	private FileAppendSingleton() {
		super();
	}

	public synchronized void appendToFile(String str, File file) {
		try {
			Files.append(str, file, Charsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static FileAppendSingleton getInstance() {
		return inst;
	}
}
