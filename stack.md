In my own search for knowledge around `NaN`, if found that the Go language specification doesn't give us more than *"as defined by the IEEE-754 standard"*. Unfortunately, the standard itself is behind a paywall.

However, this [GNU libc documentation][1] helped me a lot in understanding the principle on comparing `NaN`:

> In comparison operations, positive infinity is larger than all values
> except itself and NaN, and negative infinity is smaller than all
> values except itself and NaN. NaN is unordered: it is not equal to,
> greater than, or less than anything, including itself. x == x is false
> if the value of x is NaN. You can use this to test whether a value is
> NaN or not, but the recommended way to test for NaN is with the isnan
> function (see Floating Point Classes). In addition, <, >, <=, and >=
> will raise an exception when applied to NaNs.

As such we can see `math.IsNaN`'s [implementation][2]:

    func IsNaN(f float64) (is bool) {
    	// IEEE 754 says that only NaNs satisfy f != f.
        // ...
    	return f != f
    }

I'm adding this answer, as the `NaN` got me in trouble in a `sort.Interface` implementation and I hope it might help others.

  [1]: https://www.gnu.org/software/libc/manual/html_node/Infinity-and-NaN.html
  [2]: https://golang.org/src/math/bits.go?s=791:822#L24