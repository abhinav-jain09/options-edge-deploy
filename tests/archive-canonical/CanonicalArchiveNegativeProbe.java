/** Classify the actual thrown type; compile, missing method and incidental failures are not kills. */
public class CanonicalArchiveNegativeProbe {
    public static void main(String[] args) {
        try {
            if(args.length==1&&args[0].equals("--known-assertion"))throw new AssertionError("KNOWN_ASSERTION");
            if(args.length==1&&args[0].equals("--known-incidental"))throw new IllegalStateException("KNOWN_INCIDENTAL");
            if(args.length==1&&args[0].equals("--missing-method"))CanonicalArchiveDecoderTest.class.getDeclaredMethod("missing_method");
            CanonicalArchiveDecoderTest.main(args);
            System.out.println("{\"status\":\"PASS\"}");
        } catch(AssertionError error) {
            System.out.println("{\"status\":\"ASSERTION_KILLED\",\"type\":\""+error.getClass().getName()+"\"}");
            error.printStackTrace(System.err);System.exit(1);
        } catch(Throwable error) {
            System.out.println("{\"status\":\"INCIDENTAL_ERROR\",\"type\":\""+error.getClass().getName()+"\"}");
            error.printStackTrace(System.err);System.exit(2);
        }
    }
}
