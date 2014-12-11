import java.io.*;
import org.apache.hadoop.io.*;

public class Label implements WritableComparable<Label> {

   private Text label;
   private Text tags;
   private Text postId;

   public Label() {
      set(new Text(), new Text(), new Text());
   }

   public Label(String first, String second, String postId) {
      set(new Text(label), new Text(tags), new Text(postId));
   }
   


   public void set(Text label, Text tags, Text postId) {
      this.label = label;
      this.tags = tags;
      this.postId = postId;
   }

   public Text getLabel() {
      return label;
   }

   public Text getTags() {
      return tags;
   }
   
   public Text getPostId(){
	   return postId;
   }

   @Override
   public void write(DataOutput out) throws IOException {
	   label.write(out);
	   tags.write(out);
	   postId.write(out);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
	   label.readFields(in);
	   tags.readFields(in);
	   postId.readFields(in);
   }

   @Override
   public int hashCode() {
      return label.hashCode() * 163 + tags.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (o instanceof Label) {
    	  Label tp = (Label) o;
         return label.equals(tp.label) && tags.equals(tp.tags);
      }
      return false;
   }

   @Override
   public String toString() {
      return postId + "::" + label + "::" + tags;
   }

   @Override
   public int compareTo(Label tp) {
      int cmp = label.compareTo(tp.label);
      if (cmp != 0) {
         return cmp;
      }
      return tags.compareTo(tp.tags);
   }
}