package com.safeway.ossl.storepipeline.utility;

public final class ExceptionUtility {

    private ExceptionUtility() {}

    /** The throwable's message, or its simple class name when it has none. */
    public static String errorMessage(Throwable t) {
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }
}
