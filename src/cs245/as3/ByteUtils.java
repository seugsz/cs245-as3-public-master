package cs245.as3;

public class ByteUtils {
    //只能转非负数
    public static int BytesToUnsignedInt(byte[] nums, int index) {
        if (index + 4 > nums.length) {
            return -1;
        }
        return (((nums[index] & 0xff) << 24) | ((nums[index + 1] & 0xff) << 16) | ((nums[index + 2] & 0xff) << 8) | (nums[index + 3] & 0xff));
    }

    public static int BytesToUnsignedInt(byte[] nums) {
        return BytesToUnsignedInt(nums, 0);
    }

    //只能转非负数
    public static long BytesToUnsignedLong(byte[] nums, int index) {
        if (index + 8 > nums.length) {
            return -1;
        }
        long ans = 0;
        for (int i = 0; i < 8; i++) {
            ans |= ((nums[index + i] & 0xff) << (56 - 8 * i));
        }
        return ans;
    }

    public static long BytesToLong(byte[] nums, int index) {
        if (index + 8 > nums.length) {
            return -1;
        }
        long ans = 0;
        for (int i = 0; i < 8; i++) {
            long num = nums[index + i] < 0 ? (long) nums[index + i] + 256L : (long) nums[index + i];
            ans += num << (56 - 8 * i);
        }
        return ans;
    }
}
