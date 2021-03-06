import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as op
import torch.utils.data as Data


LR = 0.01
BATCH_SIZE = 16
EPOCH = 24
class Net(nn.Module):

    def __init__(self):
        super(Net, self).__init__()
        # print("initial...")
        # 1 input image channel, 6 output channels, 5x5 square convolution
        # kernel
        self.conv1 = nn.Conv2d(1, 6, 5)
        self.conv2 = nn.Conv2d(6, 16, 5)
        # an affine operation: y = Wx + b
        self.fc1 = nn.Linear(16 * 5 * 5, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x):
        # print("forward...")
        # nn.Sequential(
        #
        # )
        # Max pooling over a (2, 2) window
        x = F.max_pool2d(F.relu(self.conv1(x)), (2, 2))
        # If the size is a square you can only specify a single number
        x = F.max_pool2d(F.relu(self.conv2(x)), 2)
        x = x.view(-1, self.num_flat_features(x))
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x

    def num_flat_features(self, x):
        size = x.size()[1:]  # all dimensions except the batch dimension
        num_features = 1
        for s in size:
            num_features *= s
        return num_features

if __name__ == '__main__':
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    net_SGD = Net()
    net_Momentum = Net()
    net_RMSprop = Net()
    net_Adam = Net()
    nets = [net_SGD, net_Momentum, net_RMSprop, net_Adam]

    input = torch.randn(1, 1, 32, 32)
    print(type(input))
    # torch_dataset = Data.TensorDataset(input)
    loader = Data.DataLoader(dataset=input, batch_size=BATCH_SIZE, shuffle=True, num_workers=2, )

    opt_SGD = torch.optim.SGD(net_SGD.parameters(), lr=LR)
    opt_Momentum = torch.optim.SGD(net_Momentum.parameters(), lr=LR, momentum=0.8)
    opt_RMSprop = torch.optim.RMSprop(net_RMSprop.parameters(), lr=LR, alpha=0.9)
    opt_Adam = torch.optim.Adam(net_Adam.parameters(), lr=LR, betas=(0.9, 0.99))
    optimizers = [opt_SGD, opt_Momentum, opt_RMSprop, opt_Adam]

    loss_func = torch.nn.MSELoss()
    losses_his = [[], [], [], []]  # 记录 training 时不同神经网络的 loss
    target = torch.randn(10)
    target = target.view(1, -1)
    for epoch in range(EPOCH):
        for step, (b_x) in enumerate(loader):
            for net, opt, l_his in zip(nets, optimizers,  losses_his):
                out = net(b_x)
                loss = loss_func(out, target)
                opt.zero_grad()
                loss.backward()
                opt.step()
                l_his.append(loss.data.numpy())
                accuracy = float((target.data.numpy()).astype(int).sum()) / float(target.size(0))
                print('Epoch: ', epoch, '| train loss: %.4f' % loss.data.numpy(), '| test accuracy: %.2f' % accuracy)






