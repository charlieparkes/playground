class Solution:
    def twoSum(self, nums: List[int], target: int) -> List[int]:
        lookup = {}
        for i in range(len(nums)):
            try:
                return [lookup[target - nums[i]], i]
            except KeyError:
                lookup[nums[i]] = i
