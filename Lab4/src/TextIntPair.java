import java.io.*;
import org.apache.hadoop.io.*;

public class TextIntPair implements WritableComparable<TextIntPair> {

   private Text left;
   private IntWritable right;

   public TextIntPair() {
      set(new Text(), new IntWritable());
   }

   public TextIntPair(String left, int right) {
      set(new Text(left), new IntWritable(right));
   }

   public void set(Text left, IntWritable right) {
      this.left = left;
      this.right = right;
   }

   public Text getLeft() {
      return left;
   }

   public IntWritable getRight() {
      return right;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      left.write(out);
      right.write(out);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      left.readFields(in);
      right.readFields(in);
   }



   @Override
   public boolean equals(Object o) {
      if (o instanceof TextIntPair) {
    	  TextIntPair tp = (TextIntPair) o;
         return left.equals(tp.left) && right.equals(tp.right);
      }
      return false;
   }

   @Override
   public String toString() {
      return left + "\t" + right;
   }

   @Override
   public int compareTo(TextIntPair tp) {
      int cmp = left.compareTo(tp.left);
      if (cmp != 0) {
         return cmp;
      }
      return right.compareTo(tp.right);
   }
}