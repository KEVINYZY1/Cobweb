package com.xiongbeer.cobweb.saver;

import com.google.common.io.Files;
import com.xiongbeer.cobweb.conf.StaticField;
import com.xiongbeer.cobweb.exception.SaveException;
import com.xiongbeer.cobweb.utils.MD5Maker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by shaoxiong on 17-11-12.
 */
public class Slice {
    /**
     * 命名格式： filePrefix#markup[1]#md5
     */
    private static final int WRITE_LENGTH = 1024 * 1024;

    private static final char SEPARATOR = '#';

    private Set<String> content;

    private int markup;

    private String saveDirPath;

    private File localSaveFile;

    public Slice(File sliceFile) throws IOException {
        this.localSaveFile = sliceFile;
        loadSliceFile(sliceFile);
    }

    public void loadSliceFile(File sliceFile) throws IOException {
        String[] paras = sliceFile.getName().split(String.valueOf(SEPARATOR));
        markup = Integer.parseInt(paras[1]);
        content = new LinkedHashSet<>(Files.readLines(sliceFile, Charset.defaultCharset()));
    }

    public Slice(String saveDirPath, Set<String> content) {
        this.saveDirPath = saveDirPath;
        this.markup = StaticField.DEFAULT_FILTER_MARKUP;
        this.content = content;
    }

    public Slice(String saveDirPath) {
        this.saveDirPath = saveDirPath;
        this.markup = StaticField.DEFAULT_FILTER_MARKUP;
        this.content = new ConcurrentSkipListSet<>();
    }

    public Slice(String saveDirPath, int markup, Set<String> content) {
        this.saveDirPath = saveDirPath;
        this.markup = markup;
        this.content = content;
    }

    public Slice(String saveDirPath, int markup) {
        this.saveDirPath = saveDirPath;
        this.markup = markup;
        this.content = new ConcurrentSkipListSet<>();
    }

    public boolean addLine(String url) {
        return content.add(url);
    }

    public boolean deleteLine(String url) {
        return content.remove(url);
    }

    public void deleteLocalFile() {
        Optional.ofNullable(localSaveFile).ifPresent(File::delete);
    }

    public int getMarkup() {
        return markup;
    }

    public String getLocalSavePath() {
        String path = null;
        if (localSaveFile != null) {
            path = localSaveFile.getAbsolutePath();
        }
        return path;
    }

    public List<String> getContent() {
        return new ArrayList<>(content);
    }

    public String save() throws IOException {
        if (content.size() == 0) {
            throw new SaveException.SliceSaveIOException("empty slice content");
        }
        /* 临时文件名：版本4的UUID，最后会被重命名为根据它内容生成的md5值 */
        String path = saveDirPath + File.separator;
        String tempName = UUID.randomUUID().toString();
        MD5Maker md5Maker = new MD5Maker();
        File file = new File(path + tempName);
        FileOutputStream fos = new FileOutputStream(file);
        FileChannel channel = fos.getChannel();
        ByteBuffer outBuffer = ByteBuffer.allocate(WRITE_LENGTH);
        for (String url : content) {
            String line = url + System.getProperty("line.separator");
            md5Maker.update(line);
            byte[] data = line.getBytes();
            int len = data.length;
            for (int i = 0; i <= len / WRITE_LENGTH; ++i) {
                outBuffer.put(data, i * WRITE_LENGTH,
                        i == len / WRITE_LENGTH ? len % WRITE_LENGTH : WRITE_LENGTH);
                outBuffer.flip();
                channel.write(outBuffer);
                outBuffer.clear();
            }
        }
        channel.close();
        fos.close();
        String newName = StaticField.SLICE_FILE_PREFIX + SEPARATOR + markup + SEPARATOR + md5Maker.toString();
        if (!file.renameTo(new File(path + newName))) {
            throw new SaveException
                    .SliceSaveIOException("rename temp slice file failed : " + tempName + " -> " + newName);
        }
        localSaveFile = file;
        return newName;
    }
}
