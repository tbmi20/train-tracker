class Solution:
    def minWindow(self, s: str, t: str) -> str:
        if len(t) > len(s):  # trivial case where t does not fit into s
            return ""

        if len(t) == 1:  # trivial case where t is 1 char long
            return t if t in s else ""  # O(n)

        t_counts = {}  # number of times a t_char appears in t
        for i, c in enumerate(t):
            t_counts[c] = t_counts.get(c, 0) + 1
        print("t_counts", t_counts)

        result = ""
        met = 0
        i = 0
        start = None

        while i < len(s):
            if s[i] in t_counts:  # char is in t
                if start == None:  # first appearance of a t char in s
                    start = i
                if t_counts[s[i]] > 0:  # char is still needed and count>0
                    t_counts[s[i]] -= 1

                if t_counts[s[i]] == 0:  # if count for char is met
                    met += 1
                    # print(t_inds[s[i]], "popping", s[i])

                    if met == len(t_counts):  # met all counts
                        # update result if shorter or first result
                        print("s[start : i + 1]", s[start : i + 1])
                        if len(s[start : i + 1]) < len(result) or result == "":
                            result = s[start : i + 1]

                        # remove s[start] from substring and shrink left
                        t_counts[s[start]] += 1
                        met -= 1
                        start += 1
                        while s[start] not in t_counts:
                            start += 1
                        print("s[start]", s[start], "s[i]", s[i])
                        

            i += 1

        return result


solution = Solution()
s = "ADOBECODEBANC"
t = "ABC"
print(solution.minWindow(s, t))  # Output: "BANC"
