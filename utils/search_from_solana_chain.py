from solana.rpc.api import Client
from solana.keypair import Keypair
from solana.rpc.types import TokenAccountOpts
import base58

# 1. 连接Solana节点（测试网/主网）
# 测试网节点（推荐开发用）
client = Client("https://api.devnet.solana.com")
# 主网节点（生产环境）
# client = Client("https://api.mainnet-beta.solana.com")

# 2. 生成新的钱包（Keypair）
new_wallet = Keypair.generate()
# 私钥（base58格式，需妥善保存）
private_key = base58.b58encode(new_wallet.secret_key).decode('utf-8')
# 公钥（钱包地址）
public_key = new_wallet.public_key.to_base58()

print(f"钱包地址: {public_key}")
print(f"私钥: {private_key}")

# 3. 查询钱包SOL余额（测试网）
balance_response = client.get_balance(new_wallet.public_key)
balance = balance_response['result']['value'] / 10**9  # SOL的最小单位是lamport，1 SOL = 10^9 lamport
print(f"钱包余额: {balance} SOL")

# 4. （可选）请求测试网SOL（开发用）
# 测试网水龙头，给新钱包转测试SOL
try:
    airdrop_response = client.request_airdrop(new_wallet.public_key, 1 * 10**9)  # 1 SOL
    print(f"测试网领水结果: {airdrop_response}")
    # 再次查询余额
    new_balance = client.get_balance(new_wallet.public_key)['result']['value'] / 10**9
    print(f"领水后余额: {new_balance} SOL")
except Exception as e:
    print(f"领水失败: {e}")

# 5. （可选）查询SPL代币账户
opts = TokenAccountOpts(program_id="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
token_accounts = client.get_token_accounts_by_owner(new_wallet.public_key, opts)
print(f"SPL代币账户: {token_accounts}")