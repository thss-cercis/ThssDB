package cn.edu.thssdb.utils.enums;

public enum StatusCode {
    FAILURE(-1, "unknown failure"),
    SUCCESS(0, "success"),
    CONNECT_PASSWORD_FAILURE(100, "wrong password"),
    CONNECT_NOT_FOUND(101, "cannot find session");

    public int code;
    public String description;

    StatusCode(int code, String  description) {
        this.code = code;
        this.description = description;
    }
}
