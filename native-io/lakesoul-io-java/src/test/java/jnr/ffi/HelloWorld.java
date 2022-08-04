package jnr.ffi;

import jnr.ffi.LibraryLoader;

public class HelloWorld {
    public interface LibC { // A representation of libC in Java
        int puts(String s); // mapping of the puts function, in C `int puts(const char *s);`
    }

    public static void main(String[] args) {
        LibC libc = LibraryLoader.create(LibC.class).load("c"); // load the "c" library into the libc variable

        libc.puts("Hello World!"); // prints "Hello World!" to console
    }
}