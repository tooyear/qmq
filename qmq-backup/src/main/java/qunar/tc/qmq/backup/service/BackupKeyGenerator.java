package qunar.tc.qmq.backup.service;

import com.google.common.hash.Hashing;
import qunar.tc.qmq.backup.model.BackupMessage;
import qunar.tc.qmq.utils.CharsetUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yiqun.fan create on 17-10-30.
 */
public class BackupKeyGenerator {
    private static final String DATE_FORMAT_PATTERN = "yyMMddHHmmss";
    private static final String CREATE_DATE_FORMAT_PATTERN = "%10d";
    private static final long MAX_LONG = 9999999999L;


    private static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = ThreadLocal.withInitial(() -> new SimpleDateFormat(DATE_FORMAT_PATTERN));

    private final DicService dicService;

    public BackupKeyGenerator(DicService dicService) {
        this.dicService = dicService;
    }

    public byte[] generateKey(BackupMessage message) {
        String subjectId = dicService.name2Id(message.getSubject());
        String messageCreateTimeKey = generateDateKey(new Date(message.getCreateTime()));
        String messageIdKey = generateMD5Key(message.getMessageId());
        return generateRowKey(new byte[][]{toUtf8(subjectId), toUtf8(messageCreateTimeKey), toUtf8(messageIdKey)});
    }

    public byte[] generateRetryKey(BackupMessage message) {
        String subjectId = dicService.name2Id(message.getSubject());
        String messageCreateTimeKey = generateDateKey(new Date(message.getCreateTime()));
        String messageIdKey = generateMD5Key(message.getMessageId());
        return generateRowKey(new byte[][]{toUtf8(subjectId), toUtf8(messageIdKey), toUtf8(messageCreateTimeKey)});
    }

    public byte[] generatePrefixKey(String subject, long time) {
        String subjectId = dicService.name2Id(subject);
        String messageCreateTimeKey = generateDateKey(new Date(time));
        return generateRowKey(new byte[][]{toUtf8(subjectId), toUtf8(messageCreateTimeKey)});
    }

    private String generateDateKey(Date key) {
        String formatText = DATE_FORMATTER.get().format(key);
        Long dateLong = MAX_LONG - Long.parseLong(formatText);
        formatText = String.format(CREATE_DATE_FORMAT_PATTERN, dateLong);
        return formatText;
    }

    public byte[] generateRowKey(byte[][] parts) {
        int len = 0;
        for (byte[] part : parts) {
            len += part.length;
        }

        byte[] key = new byte[len];
        int writerIndex = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, key, writerIndex, part.length);
            writerIndex += part.length;
        }
        return key;
    }


    private String generateMD5Key(byte[] key) {
        return Hashing.md5().hashBytes(key).toString();
    }

    private byte[] toUtf8(final String s) {
        return CharsetUtils.toUTF8Bytes(s);
    }
}
